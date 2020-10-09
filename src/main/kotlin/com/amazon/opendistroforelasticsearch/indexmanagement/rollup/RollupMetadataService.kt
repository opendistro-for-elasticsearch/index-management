/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.transport.RemoteTransportException
import java.time.Instant

// Service that handles CRUD operations for rollup metadata
// TODO: Metadata should be stored on same shard as its owning rollup job using routing
// TODO: This whole class needs to be cleaned up
class RollupMetadataService(val client: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

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
        val nextWindowStartTime = getStartTime(rollup)
        val nextWindowEndTime = nextWindowStartTime // TODO: This needs to be calculated, but it needs to take into account calendar vs fixed
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
    private suspend fun getStartTime(rollup: Rollup): Instant {
        val dateHistogram = rollup.dimensions.first() as DateHistogram // rollup requires the first dimension to be the date histogram
        val searchSourceBuilder = SearchSourceBuilder()
            .trackTotalHits(false)
            .size(1)
            .query(MatchAllQueryBuilder())
            .sort(dateHistogram.sourceField, SortOrder.ASC) // TODO: figure out where nulls are sorted
            .fetchSource(dateHistogram.sourceField, null)
        val searchRequest = SearchRequest(rollup.sourceIndex)
            .source(searchSourceBuilder)
            .allowPartialSearchResults(false)

        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        // TODO: Timezone?
        val firstSourceOrNull = response.hits.hits.firstOrNull()?.sourceAsMap?.get(dateHistogram.sourceField) as String?
        return requireNotNull(firstSourceOrNull?.let { Instant.parse(it) }) {
            "Could not find the start time for ${dateHistogram.sourceField} on the document"
        }
    }

    // This updates the metadata for a continuous rollup after an execution of the composite search and ingestion of rollup data
    fun getUpdatedContinuousMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        internalComposite: InternalComposite
    ): RollupMetadata {
        // TODO: finished, failed, failure reason
        val dateHistogram = rollup.dimensions.first() as DateHistogram
        val intervalString = (dateHistogram.calendarInterval ?: dateHistogram.fixedInterval) as String
        // TODO this doesn't seem to be accurate for calendar interval - how inaccurate is it and whats a diff way we can do this
        //  as this would affect the windows of time which is a big issue.
        val interval = DateHistogramInterval(intervalString)
        val afterKey = internalComposite.afterKey()
        val millis = interval.estimateMillis()
        // TODO: get rid of !!
        val nextStart = if (afterKey == null) {
            Instant.ofEpochMilli(metadata.continuous!!.nextWindowStartTime.toEpochMilli()).plusMillis(millis)
        } else {
            metadata.continuous!!.nextWindowStartTime
        }
        val nextEnd = if (afterKey == null) {
            Instant.ofEpochMilli(metadata.continuous.nextWindowEndTime.toEpochMilli()).plusMillis(millis)
        } else {
            metadata.continuous.nextWindowEndTime
        }
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

    // If the job does not have a metadataID then we need to initialize the first metadata
    // document for this job otherwise we should get the existing metadata document
    suspend fun init(rollup: Rollup): RollupMetadata {
        if (rollup.metadataID != null) {
            // TODO: How does the user recover from the not found error?
            return getExistingMetadata(rollup.metadataID)
                ?: update(RollupMetadata(rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.FAILED,
                    failureReason = "Not able to get the rollup metadata [${rollup.metadataID}]"), false)
        }
        val metadata = if (rollup.continuous) createContinuousMetadata(rollup) else createNonContinuousMetadata(rollup)
        return update(metadata, false)
    }

    suspend fun updateMetadata(rollup: Rollup, metadata: RollupMetadata, internalComposite: InternalComposite): RollupMetadata {
        val updatedMetadata = if (rollup.continuous) {
            getUpdatedContinuousMetadata(rollup, metadata, internalComposite)
        } else {
            getUpdatedNonContinuousMetadata(rollup, metadata, internalComposite)
        }
        return update(updatedMetadata, metadata.id != RollupMetadata.NO_ID)
    }

    // TODO: error handling, make sure to handle RTE for pretty much everything..
    // TODO: Clean this up
    suspend fun update(metadata: RollupMetadata, updating: Boolean): RollupMetadata {
        @Suppress("BlockingMethodInNonBlockingContext")
        val builder = XContentFactory.jsonBuilder().startObject().field(RollupMetadata.ROLLUP_METADATA_TYPE, metadata).endObject()
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
