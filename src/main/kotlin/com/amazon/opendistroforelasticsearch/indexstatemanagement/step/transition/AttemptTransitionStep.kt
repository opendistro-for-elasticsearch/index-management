/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.transition

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.TransitionsActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AttemptTransitionStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: TransitionsActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)

    override suspend fun execute() {
        val statsRequest = IndicesStatsRequest()
                .indices(managedIndexMetaData.index).clear().docs(true)
        val stats: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }
        val indexCreationDate = clusterService.state().metaData().index(managedIndexMetaData.index).creationDate
        val numDocs = stats.primaries.docs.count
        val indexSize = stats.primaries.docs.totalSizeInBytes
        logger.info("""
            Current index stats are:
            indexCreationDate: $indexCreationDate
            numDocs: $numDocs
            indexSize: $indexSize
        """.trimIndent())

        // TODO: Evaluate transition conditions
        // TODO: Update managed index meta data
    }

    companion object {
        const val name = "attempt_transition"
    }
}
