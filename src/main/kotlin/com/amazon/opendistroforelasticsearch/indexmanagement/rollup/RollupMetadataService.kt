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
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ContinuousMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata.Companion.NO_ID
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.DATE_FIELD_EPOCH_MILLIS_FORMAT
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
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
import org.elasticsearch.transport.RemoteTransportException
import java.time.Instant

// TODO: Wrap client calls in retry for transient failures
// Service that handles CRUD operations for rollup metadata
@Suppress("TooManyFunctions")
class RollupMetadataService(val client: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    // If the job does not have a metadataID then we need to initialize the first metadata
    // document for this job otherwise we should get the existing metadata document
    @Suppress("ReturnCount", "ComplexMethod", "NestedBlockDepth")
    suspend fun init(rollup: Rollup): MetadataResult {
        if (rollup.metadataID != null) {
            val existingMetadata = when (val getMetadataResult = getExistingMetadata(rollup)) {
                is MetadataResult.Success -> getMetadataResult.metadata
                is MetadataResult.NoMetadata -> null
                is MetadataResult.Failure -> return getMetadataResult
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
                // The existing metadata was not found, create a new metadata in FAILED status
                return submitMetadataUpdate(RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(),
                    status = RollupMetadata.Status.FAILED, failureReason = "Not able to get the rollup metadata [${rollup.metadataID}]",
                    stats = RollupStats(0, 0, 0, 0, 0)), false)
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

    @Suppress("ReturnCount")
    private suspend fun recoverRetryMetadata(rollup: Rollup, metadata: RollupMetadata): MetadataResult {
        var continuousMetadata = metadata.continuous
        if (rollup.continuous && metadata.continuous == null) {
            val nextWindowStartTime = when (val initStartTimeResult = getInitialStartTime(rollup)) {
                is StartingTimeResult.Success -> initStartTimeResult.startingTime
                is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata
                is StartingTimeResult.Failure ->
                    return MetadataResult.Failure("Failed to initialize start time for retried rollup job [${rollup.id}]", initStartTimeResult.e)
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
        metadata: RollupMetadata,
        internalComposite: InternalComposite
    ): RollupMetadata {
        val afterKey = internalComposite.afterKey()
        return metadata.copy(
            afterKey = afterKey,
            lastUpdatedTime = Instant.now(),
            status = if (afterKey == null) RollupMetadata.Status.FINISHED else RollupMetadata.Status.STARTED
        )
    }

    // This returns the first instantiation of a RollupMetadata for a continuous rollup
    @Suppress("ReturnCount")
    private suspend fun createContinuousMetadata(rollup: Rollup): MetadataResult {
        val nextWindowStartTime = when (val initStartTimeResult = getInitialStartTime(rollup)) {
            is StartingTimeResult.Success -> initStartTimeResult.startingTime
            is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata
            is StartingTimeResult.Failure ->
                return MetadataResult.Failure("Failed to initialize start time for rollup [${rollup.id}]", initStartTimeResult.e)
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

    // TODO: Let User specify their own filter query that is applied to the composite agg search
    @Suppress("ReturnCount")
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
                .fetchSource(false)
                .docValueField(dateHistogram.sourceField, DATE_FIELD_EPOCH_MILLIS_FORMAT)
            val searchRequest = SearchRequest(rollup.sourceIndex)
                .source(searchSourceBuilder)
                .allowPartialSearchResults(false)
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            if (response.hits.hits.isEmpty()) {
                // Empty doc hits will result in a no-op from the runner
                return StartingTimeResult.NoDocumentsFound
            }

            // Get the doc value field of the dateHistogram.sourceField for the first search hit converted to epoch millis
            // If the doc value is null or empty it will be treated the same as empty doc hits
            val firstHitTimestamp = response.hits.hits.first().field(dateHistogram.sourceField).getValue<String>()?.toLong()
                ?: return StartingTimeResult.NoDocumentsFound

            return StartingTimeResult.Success(getRoundedTime(firstHitTimestamp, dateHistogram))
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.debug("Error when getting initial start time for rollup [${rollup.id}]: $unwrappedException")
            return StartingTimeResult.Failure(unwrappedException)
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
    private fun getRoundedTime(timestamp: Long, dateHistogram: DateHistogram): Instant {
        val roundingStrategy = getRoundingStrategy(dateHistogram)
        val roundedMillis = roundingStrategy
            .prepare(timestamp, timestamp)
            .round(timestamp)
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
            val timeValue = TimeValue.parseTimeValue(intervalString, "RollupMetadataService#getRoundingStrategy")
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

    @Suppress("BlockingMethodInNonBlockingContext", "ReturnCount")
    suspend fun getExistingMetadata(rollup: Rollup): MetadataResult {
        val errorMessage = "Error when getting rollup metadata [${rollup.metadataID}]"
        try {
            var rollupMetadata: RollupMetadata? = null
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }

            if (!response.isExists) return MetadataResult.NoMetadata

            val metadataSource = response.sourceAsBytesRef
            metadataSource?.let {
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                    rollupMetadata = xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                }
            }

            return if (rollupMetadata != null) {
                MetadataResult.Success(rollupMetadata!!)
            } else MetadataResult.NoMetadata
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.debug("$errorMessage: $unwrappedException")
            return MetadataResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.debug("$errorMessage: $e")
            return MetadataResult.Failure(errorMessage, e)
        }
    }

    suspend fun updateMetadata(rollup: Rollup, metadata: RollupMetadata, internalComposite: InternalComposite): RollupMetadata {
        val updatedMetadata = if (rollup.continuous) {
            getUpdatedContinuousMetadata(rollup, metadata, internalComposite)
        } else {
            getUpdatedNonContinuousMetadata(metadata, internalComposite)
        }

        return updateMetadata(updatedMetadata)
    }

    suspend fun updateMetadata(metadata: RollupMetadata): RollupMetadata {
        return when (val metadataUpdateResult = submitMetadataUpdate(metadata, metadata.id != NO_ID)) {
            is MetadataResult.Success -> metadataUpdateResult.metadata
            is MetadataResult.Failure ->
                throw RollupMetadataException("Failed to update rollup metadata [${metadata.id}]", metadataUpdateResult.cause)
            // NoMetadata is not expected from submitMetadataUpdate here
            is MetadataResult.NoMetadata -> throw RollupMetadataException("Unexpected state when updating rollup metadata [${metadata.id}]", null)
        }
    }

    /**
     * Sets a failure metadata for the rollup job with the given reason.
     * Can provide an existing metadata to update, if none are provided a new metadata is created
     * to replace the current one for the job.
     */
    suspend fun setFailedMetadata(job: Rollup, reason: String, existingMetadata: RollupMetadata? = null): MetadataResult {
        val updatedMetadata: RollupMetadata?
        if (existingMetadata == null) {
            // Create new metadata
            updatedMetadata = RollupMetadata(
                rollupID = job.id,
                status = RollupMetadata.Status.FAILED,
                failureReason = reason,
                lastUpdatedTime = Instant.now(),
                stats = RollupStats(0, 0, 0, 0, 0)
            )
        } else {
            // Update the given existing metadata
            updatedMetadata = existingMetadata.copy(
                status = RollupMetadata.Status.FAILED,
                failureReason = reason,
                lastUpdatedTime = Instant.now()
            )
        }

        return submitMetadataUpdate(updatedMetadata, updatedMetadata.id != NO_ID)
    }

    @Suppress("ComplexMethod", "ReturnCount")
    private suspend fun submitMetadataUpdate(metadata: RollupMetadata, updating: Boolean): MetadataResult {
        val errorMessage = "An error occurred when ${if (updating) "updating" else "creating"} rollup metadata"
        try {
            @Suppress("BlockingMethodInNonBlockingContext")
            val builder = XContentFactory.jsonBuilder().startObject()
                .field(RollupMetadata.ROLLUP_METADATA_TYPE, metadata)
                .endObject()
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(builder).routing(metadata.rollupID)
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
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            return MetadataResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            return MetadataResult.Failure(errorMessage, e)
        }
    }
}

sealed class MetadataResult {
    // A successful MetadataResult just means a metadata was returned,
    // it can still have a FAILED status
    data class Success(val metadata: RollupMetadata) : MetadataResult()
    data class Failure(val message: String = "An error occurred for rollup metadata", val cause: Exception) : MetadataResult()
    object NoMetadata : MetadataResult()
}

sealed class StartingTimeResult {
    data class Success(val startingTime: Instant) : StartingTimeResult()
    data class Failure(val e: Exception) : StartingTimeResult()
    object NoDocumentsFound : StartingTimeResult()
}

class RollupMetadataException(message: String, cause: Throwable?) : Exception(message, cause)
