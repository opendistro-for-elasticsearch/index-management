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

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupSearchRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import java.time.Instant

// TODO: Backoff/Throttling when cluster is overloaded - CircuitBreakingException?
//  A cluster level setting to control how many rollups can run at once? Or should we be skipping when cpu/memory/jvm is high?
// Deals with know when and how to search the source index
// Knowing when means dealing with time windows and whether or not enough time has passed
// Knowing how means converting the rollup configuration into a composite aggregation
class RollupSearchService(val client: Client) {

    private val logger = LogManager.getLogger(javaClass)

    fun shouldProcessWindow(rollup: Rollup, metadata: RollupMetadata): Boolean { // TODO: Failed shouldn't process? How to recover from failed -> how does a user retry a failed rollup
        // For both continuous and non-continuous rollups if there is an afterKey it means we are still
        // processing data from the current window and should continue to process, the only way we ended up here with an
        // afterKey is if we were still processing data is if the job somehow stopped and was rescheduled (i.e. node crashed etc.)
        if (metadata.afterKey != null) return true // TODO: What if there is afterKey and we are in failed/stopped? What takes priority?
        if (!rollup.continuous) {
            if (metadata.status == RollupMetadata.Status.INIT) return true
            // If a non-continuous rollup job does not have an afterKey and is not in INIT then
            // it is either finished, failed, stopped or in running which should not be possible if afterKey is null
            logger.debug("Non-continuous job [${rollup.id}] is not processing next window [$metadata]")
            return false
        } else {
            return hasNextFullWindow(metadata) // TODO: Behavior when next full window but 0 docs/afterkey is null
        }
    }

    private fun hasNextFullWindow(metadata: RollupMetadata): Boolean {
        return Instant.now().isAfter(metadata.nextWindowEndTime)
    }

    // TODO: error handling
    suspend fun executeCompositeSearch(job: Rollup, metadata: RollupMetadata): InternalComposite {
        val response: SearchResponse = client.suspendUntil { search(job.getRollupSearchRequest(metadata), it) }
        return response.aggregations.get(job.id)
    }
}
