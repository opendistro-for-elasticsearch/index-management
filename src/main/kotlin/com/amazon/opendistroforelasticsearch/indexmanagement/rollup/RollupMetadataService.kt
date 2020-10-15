/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ContinuousMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.Rounding
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import java.time.Instant
import java.time.ZonedDateTime

// Service that handles CRUD operations for rollup metadata
// TODO: Metadata should be stored on same shard as its owning rollup job using routing
// TODO: This whole class needs to be cleaned up
@Suppress("TooManyFunctions")
class RollupMetadataService(val client: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    // If the job does not have a metadataID then we need to initialize the first metadata
    // document for this job otherwise we should get the existing metadata document
    suspend fun init(rollup: Rollup): MetadataResult {
        if (rollup.metadataID != null) {
            // TODO: How does the user recover from the not found error?
            val existingMetadata = when (val getMetadataResult = getExistingMetadata(rollup.metadataID)) {
                is MetadataResult.Success -> getMetadataResult.metadata
                is MetadataResult.NoMetadata -> null
                // TODO: Possibly update RollupMetadataException to take an exception so we can pass in "getMetadataResult.e"
                //   Or can return MetadataResult.Failure here and update it to take in a "message" and exception
                is MetadataResult.Failure -> throw RollupMetadataException("Error getting existing metadata during init")
            }

            if (existingMetadata != null) {
                if (existingMetadata.status == RollupMetadata.Status.RETRY) {
                    val recoveredMetadata = when (val recoverMetadataResult = recoverRetryMetadata(rollup, existingMetadata)) {
                        is MetadataResult.Success -> recoverMetadataResult.metadata
                        // NoMetadata here means that there were no documents when initializing start time
                        // for a continuous rollup so we will propagate the response to no-op in the runner
                        is MetadataResult.NoMetadata -> return recoverMetadataResult
                        // In case of failure, return early with the result
                        is MetadataResult.Failure -> return recoverMetadataResult
                    }

                    // Update to the recovered metadata if recovery was successful
                    return submitMetadataUpdate(recoveredMetadata, true)
                } else {
                    // If metadata exists and was not in RETRY status, return the existing metadata
                    return MetadataResult.Success(existingMetadata)
                }
            } else {
                submitMetadataUpdate(RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.FAILED,
                    failureReason = "Not able to get the rollup metadata [${rollup.metadataID}]", stats = RollupStats(0, 0, 0, 0, 0)), false)
            }
        }

        val createdMetadataResult = if (rollup.continuous) createContinuousMetadata(rollup) else createNonContinuousMetadata(rollup)
        return when (createdMetadataResult) {
            is MetadataResult.Success -> submitMetadataUpdate(createdMetadataResult.metadata, false)
            // Hitting this case means that there were no documents when initializing start time for a continuous rollup
            is MetadataResult.NoMetadata -> createdMetadataResult
            is MetadataResult.Failure -> createdMetadataResult
        }
    }

    private suspend fun recoverRetryMetadata(rollup: Rollup, metadata: RollupMetadata): MetadataResult {
        var continuousMetadata = metadata.continuous
        if (rollup.continuous && metadata.continuous == null) {
            val nextWindowStartTime = when (val initStartTimeResult = getInitialStartTime(rollup)) {
                is StartingTimeResult.Success -> initStartTimeResult.startingTime
                is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata
                // TODO: Probably change this to return MetadataResult.Failure() with a message so that
                //   runner can be the one to throw the exception
                is StartingTimeResult.Failure -> throw RollupMetadataException("Failed to initialize start time for retried job")
            }
            val nextWindowEndTime = getShiftedTime(nextWindowStartTime, rollup)
            continuousMetadata = ContinuousMetadata(nextWindowStartTime, nextWindowEndTime)
        }

        return MetadataResult.Success(
            metadata.copy(
                continuous = continuousMetadata,
                status = RollupMetadata.Status.STARTED
            )
        )
    }

    // This returns the first instantiation of a RollupMetadata for a non-continuous rollup
    private fun createNonContinuousMetadata(rollup: Rollup): MetadataResult =
        MetadataResult.Success(RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.INIT,
            stats = RollupStats(0, 0, 0, 0, 0)))

    // This updates the metadata for a non-continuous rollup after an execution of the composite search and ingestion of rollup data
    private fun getUpdatedNonContinuousMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        internalComposite: InternalComposite
    ): RollupMetadata {
        // TODO: finished, failed, failure reason
        val afterKey = internalComposite.afterKey()
        return metadata.copy(
            afterKey = afterKey,
            lastUpdatedTime = Instant.now(),
            status = if (afterKey == null) RollupMetadata.Status.FINISHED else RollupMetadata.Status.STARTED
        )
    }

    // This returns the first instantiation of a RollupMetadata for a continuous rollup
    private suspend fun createContinuousMetadata(rollup: Rollup): MetadataResult {
        val nextWindowStartTime = when (val initStartTimeResult = getInitialStartTime(rollup)) {
            is StartingTimeResult.Success -> initStartTimeResult.startingTime
            is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata
            is StartingTimeResult.Failure -> throw RollupMetadataException("Failed to initialize start time")
        }
        // The first end time is just the next window start time
        val nextWindowEndTime = getShiftedTime(nextWindowStartTime, rollup)
        return MetadataResult.Success(
            RollupMetadata(
                rollupID = rollup.id,
                afterKey = null,
                lastUpdatedTime = Instant.now(),
                continuous = ContinuousMetadata(nextWindowStartTime, nextWindowEndTime),
                status = RollupMetadata.Status.INIT,
                failureReason = null,
                stats = RollupStats(0, 0, 0, 0, 0)
            )
        )
    }

    // TODO: Let user specify when the first window should start at? i.e. they want daily rollups and they want it exactly at 12:00AM -> 11:59PM
    //  but their first event is at 5:35PM, or maybe they want to rollup just the past months worth of data but there is a years worth in index?
    //  Could perhaps solve that by allowing the user to specify their own filter query that is applied to the composite agg search
    // TODO: handle exception
    @Throws(Exception::class)
    private suspend fun getInitialStartTime(rollup: Rollup): StartingTimeResult {
        try {
            // Rollup requires the first dimension to be the date histogram
            val dateHistogram = rollup.dimensions.first() as DateHistogram
            val searchSourceBuilder = SearchSourceBuilder()
                .size(1)
                .query(MatchAllQueryBuilder())
                .sort(dateHistogram.sourceField, SortOrder.ASC) // TODO: figure out where nulls are sorted
                .trackTotalHits(false)
                .fetchSource(dateHistogram.sourceField, null)
            val searchRequest = SearchRequest(rollup.sourceIndex)
                .source(searchSourceBuilder)
                .allowPartialSearchResults(false)
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            if (response.hits.hits.isEmpty()) {
                // TODO: Handle empty hits (should result in no-op for runner)
                return StartingTimeResult.NoDocumentsFound
            }

            // TODO: Empty result case has been checked for, are there any other cases where this can fail (such as failing cast)?
            val firstSource = response.hits.hits.first().sourceAsMap[dateHistogram.sourceField] as String

            return StartingTimeResult.Success(getRoundedTime(firstSource, dateHistogram))
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.debug("Error when getting initial start time for rollup [${rollup.id}]: $e")
            return StartingTimeResult.Failure(e)
        }
    }

    /**
     * Return time rounded down to the nearest unit of time the interval is based on.
     * This should map to the equivalent bucket a document with the given timestamp would fall into for the date histogram.
     */
    private fun getRoundedTime(timestamp: String, dateHistogram: DateHistogram): Instant {
        val roundingStrategy = getRoundingStrategy(dateHistogram)
        val timeInMillis = ZonedDateTime.parse(timestamp).toInstant().toEpochMilli()

        val roundedMillis = roundingStrategy
            .prepare(timeInMillis, timeInMillis)
            .round(timeInMillis)
        return Instant.ofEpochMilli(roundedMillis)
    }

    /** Takes an existing start or end time and returns the value for the next window based on the rollup interval */
    private fun getShiftedTime(time: Instant, rollup: Rollup): Instant {
        val dateHistogram = rollup.dimensions.first() as DateHistogram
        val roundingStrategy = getRoundingStrategy(dateHistogram)

        val timeInMillis = time.toEpochMilli()
        val nextRoundedMillis = roundingStrategy
            .prepare(timeInMillis, timeInMillis)
            .nextRoundingValue(timeInMillis)
        return Instant.ofEpochMilli(nextRoundedMillis)
    }

    /**
     * Get the rounding strategy for the given time interval in the DateHistogram.
     * This is used to calculate time windows by rounding the given time based on the interval.
     */
    // TODO: Could make this an extension function of DateHistogram and add to some utility file
    private fun getRoundingStrategy(dateHistogram: DateHistogram): Rounding {
        val intervalString = (dateHistogram.calendarInterval ?: dateHistogram.fixedInterval) as String
        // TODO: Make sure the interval string is validated before getting here so we don't get errors
        return if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(intervalString)) {
            // Calendar intervals should be handled here
            val intervalUnit: Rounding.DateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS[intervalString]!!
            Rounding.builder(intervalUnit)
                .timeZone(dateHistogram.timezone)
                .build()
        } else {
            // Fixed intervals are handled here
            val timeValue = TimeValue.parseTimeValue(intervalString, "RollupMetadataService#getTimeInterval")
            Rounding.builder(timeValue)
                .timeZone(dateHistogram.timezone)
                .build()
        }
    }

    // This updates the metadata for a continuous rollup after an execution of the composite search and ingestion of rollup data
    fun getUpdatedContinuousMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        internalComposite: InternalComposite
    ): RollupMetadata {
        // TODO: finished, failed, failure reason
        val afterKey = internalComposite.afterKey()
        // TODO: get rid of !!
        val nextStart = if (afterKey == null) {
            getShiftedTime(metadata.continuous!!.nextWindowStartTime, rollup)
        } else metadata.continuous!!.nextWindowStartTime
        val nextEnd = if (afterKey == null) {
            getShiftedTime(metadata.continuous.nextWindowEndTime, rollup)
        } else metadata.continuous.nextWindowEndTime
        return metadata.copy(
            afterKey = internalComposite.afterKey(),
            lastUpdatedTime = Instant.now(),
            continuous = ContinuousMetadata(nextStart, nextEnd),
            status = RollupMetadata.Status.STARTED
        )
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun getExistingMetadata(id: String): MetadataResult {
        try {
            var rollupMetadata: RollupMetadata? = null
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, id)
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }

            if (!response.isExists) return MetadataResult.NoMetadata

            val metadataSource = response.sourceAsBytesRef
            metadataSource?.let {
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                    rollupMetadata = RollupMetadata.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
                }
            }

            return if (rollupMetadata != null) {
                MetadataResult.Success(rollupMetadata!!)
            } else MetadataResult.NoMetadata
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.debug("Error when getting rollup metadata [$id]")
            return MetadataResult.Failure(e)
        }
    }

    suspend fun updateMetadata(rollup: Rollup, metadata: RollupMetadata, internalComposite: InternalComposite): RollupMetadata {
        val updatedMetadata = if (rollup.continuous) {
            getUpdatedContinuousMetadata(rollup, metadata, internalComposite)
        } else {
            getUpdatedNonContinuousMetadata(rollup, metadata, internalComposite)
        }

        return when (val metadataUpdateResult = submitMetadataUpdate(updatedMetadata, metadata.id != RollupMetadata.NO_ID)) {
            is MetadataResult.Success -> metadataUpdateResult.metadata
            is MetadataResult.Failure -> throw RollupMetadataException("Failed to update rollup metadata [${metadata.id}]")
            // NoMetadata is not expected from submitMetadataUpdate here
            is MetadataResult.NoMetadata -> throw RollupMetadataException("Unexpected state when updating rollup metadata [${metadata.id}]")
        }
    }

    // TODO: error handling, make sure to handle RTE for pretty much everything..
    // TODO: Clean this up
    suspend fun submitMetadataUpdate(metadata: RollupMetadata, updating: Boolean): MetadataResult {
        @Suppress("BlockingMethodInNonBlockingContext")
        try {
            val builder = XContentFactory.jsonBuilder().startObject()
                .field(RollupMetadata.ROLLUP_METADATA_TYPE, metadata)
                .endObject()
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(builder)
            if (updating) {
                indexRequest.id(metadata.id).setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            var status: RollupMetadata.Status = metadata.status
            var failureReason: String? = metadata.failureReason
            when (response.result) {
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    // noop
                }
                DocWriteResponse.Result.DELETED, DocWriteResponse.Result.NOOP, DocWriteResponse.Result.NOT_FOUND, null -> {
                    status = RollupMetadata.Status.FAILED
                    failureReason = "The create metadata call failed with a ${response.result?.lowercase} result"
                }
            }
            // TODO: Is seqno/prim and id returned for all?
            return MetadataResult.Success(
                metadata.copy(
                    id = response.id,
                    seqNo = response.seqNo,
                    primaryTerm = response.primaryTerm,
                    status = status,
                    failureReason = failureReason
                )
            )
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.debug("Error when ${if (updating) "updating" else "creating"} rollup metadata")
            return MetadataResult.Failure(e)
        }
    }

    sealed class MetadataResult {
        // A successful MetadataResult just means a metadata was returned,
        // it can still have a FAILED status
        data class Success(val metadata: RollupMetadata) : MetadataResult()
        data class Failure(val e: Exception) : MetadataResult()
        object NoMetadata : MetadataResult()
    }

    sealed class StartingTimeResult {
        data class Success(val startingTime: Instant) : StartingTimeResult()
        data class Failure(val e: Exception) : StartingTimeResult()
        object NoDocumentsFound : StartingTimeResult()
    }

    class RollupMetadataException(message: String) : Exception(message)
}
