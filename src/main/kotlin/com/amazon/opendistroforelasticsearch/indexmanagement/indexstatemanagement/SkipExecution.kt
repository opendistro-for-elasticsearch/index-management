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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ClusterStateListener
import org.elasticsearch.cluster.service.ClusterService

// TODO this can be moved to job scheduler, so that all extended plugin
//  can avoid running jobs in an upgrading cluster
@OpenForTesting
class SkipExecution(
    private val client: Client,
    private val clusterService: ClusterService
) : ClusterStateListener {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile final var flag: Boolean = false
        private set

    init {
        clusterService.addListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (event.nodesChanged() || event.isNewCluster) {
            sweepISMPluginVersion()
        }
    }

    fun sweepISMPluginVersion() {
        // if old version ISM plugin exists (2 versions ISM in one cluster), set skip flag to true
        val request = NodesInfoRequest().clear().addMetric("plugins")
        client.execute(NodesInfoAction.INSTANCE, request, object : ActionListener<NodesInfoResponse> {
            override fun onResponse(response: NodesInfoResponse) {
                val versionSet = mutableSetOf<String>()

                response.nodes.map { it.getInfo(PluginsAndModules::class.java).pluginInfos }
                    .forEach { it.forEach { nodePlugin ->
                        if (nodePlugin.name == "opendistro-index-management" ||
                            nodePlugin.name == "opendistro_index_management") {
                            versionSet.add(nodePlugin.version)
                        }
                    } }

                if (versionSet.size > 1) {
                    flag = true
                    logger.info("There are multiple versions of Index Management plugins in the cluster: $versionSet")
                } else flag = false
            }

            override fun onFailure(e: Exception) {
                logger.error("Failed sweeping nodes for ISM plugin versions: $e")
                flag = false
            }
        })
    }
}
