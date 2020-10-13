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
    suspend fun init(rollup: Rollup): RollupMetadata {
        if (rollup.metadataID != null) {
            // TODO: How does the user recover from the not found error?
            return getExistingMetadata(rollup.metadataID)
                ?: submitMetadataUpdate(RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.FAILED,
                    failureReason = "Not able to get the rollup metadata [${rollup.metadataID}]"), false)
        }
        val metadata = if (rollup.continuous) createContinuousMetadata(rollup) else createNonContinuousMetadata(rollup)
        return submitMetadataUpdate(metadata, false)
    }

    // This returns the first instantiation of a RollupMetadata for a non-continuous rollup
    private fun createNonContinuousMetadata(rollup: Rollup): RollupMetadata =
        RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.INIT)

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
    private suspend fun createContinuousMetadata(rollup: Rollup): RollupMetadata {
        val nextWindowStartTime = getInitialStartTime(rollup)
        val nextWindowEndTime = getShiftedTime(nextWindowStartTime, rollup) // The first end time is just the next window start time
        return RollupMetadata(
            rollupID = rollup.id,
            afterKey = null,
            lastUpdatedTime = Instant.now(),
            continuous = ContinuousMetadata(nextWindowStartTime, nextWindowEndTime),
            status = RollupMetadata.Status.INIT,
            failureReason = null
        )
    }

    // TODO: Let user specify when the first window should start at? i.e. they want daily rollups and they want it exactly at 12:00AM -> 11:59PM
    //  but their first event is at 5:35PM, or maybe they want to rollup just the past months worth of data but there is a years worth in index?
    //  Could perhaps solve that by allowing the user to specify their own filter query that is applied to the composite agg search
    // TODO: handle exception
    @Throws(Exception::class)
    private suspend fun getInitialStartTime(rollup: Rollup): Instant {
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
        }

        // TODO: Empty result case has been checked for, are there any other cases where this can fail (such as failing cast)?
        val firstSource = response.hits.hits.first().sourceAsMap[dateHistogram.sourceField] as String

        return getRoundedTime(firstSource, dateHistogram)
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
    suspend fun getExistingMetadata(id: String): RollupMetadata? {
        var rollupMetadata: RollupMetadata? = null
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, id)
        val response: GetResponse = client.suspendUntil { get(getRequest, it) }

        if (!response.isExists) return rollupMetadata

        val metadataSource = response.sourceAsBytesRef
        metadataSource?.let {
            withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                rollupMetadata = RollupMetadata.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
            }
        }

        return rollupMetadata
    }

    suspend fun updateMetadata(rollup: Rollup, metadata: RollupMetadata, internalComposite: InternalComposite): RollupMetadata {
        val updatedMetadata = if (rollup.continuous) {
            getUpdatedContinuousMetadata(rollup, metadata, internalComposite)
        } else {
            getUpdatedNonContinuousMetadata(rollup, metadata, internalComposite)
        }
        return submitMetadataUpdate(updatedMetadata, metadata.id != RollupMetadata.NO_ID)
    }

    // TODO: error handling, make sure to handle RTE for pretty much everything..
    // TODO: Clean this up
    suspend fun submitMetadataUpdate(metadata: RollupMetadata, updating: Boolean): RollupMetadata {
        @Suppress("BlockingMethodInNonBlockingContext")
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
        return metadata.copy(
            id = response.id,
            seqNo = response.seqNo,
            primaryTerm = response.primaryTerm,
            status = status,
            failureReason = failureReason
        )
    }
}
