package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.OpenForTesting
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

@OpenForTesting
class SkipExecution(
    private val client: Client,
    private val clusterService: ClusterService
) : ClusterStateListener {
    private val logger = LogManager.getLogger(javaClass)

    final var flag: Boolean = false
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
        // if old node exits, set skip flag to true
        val request = NodesInfoRequest().clear().addMetric("plugins")
        client.execute(NodesInfoAction.INSTANCE, request,
                object : ActionListener<NodesInfoResponse> {
                    override fun onResponse(response: NodesInfoResponse) {
                        flag = false
                        val versionSet = mutableSetOf<String>()
                        for (node in response.nodes) {
                            val pluginsInfo = node.getInfo(PluginsAndModules::class.java).pluginInfos
                            pluginsInfo.forEach {
                                if (it.name == "opendistro_index_management") {
                                    versionSet.add(it.version)
                                    if (versionSet.size > 1) flag = true
                                }
                            }
                        }
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("failed when get node info for setting skip flag: $e")
                    }
                })
    }
}
