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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestDeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestGetPolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestIndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsFilter
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

internal class IndexStateManagementPlugin : JobSchedulerExtension, ActionPlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexStateManagementIndices: IndexStateManagementIndices

    companion object {
        const val PLUGIN_NAME = "opendistro-ism"
        const val POLICY_BASE_URI = "/_opendistro/_ism/policies"
        const val INDEX_STATE_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_STATE_MANAGEMENT_JOB_TYPE = "opendistro-managed-index"
    }

    override fun getJobIndex(): String {
        return INDEX_STATE_MANAGEMENT_INDEX
    }

    override fun getJobType(): String {
        return INDEX_STATE_MANAGEMENT_JOB_TYPE
    }

    override fun getJobRunner(): ScheduledJobRunner {
        return ManagedIndexRunner.instance
    }

    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, version ->
            var job: ScheduledJobParameter? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.MANAGED_INDEX_TYPE -> job = ManagedIndexConfig.parse(xcp, id, version)
                    Policy.POLICY_TYPE -> job = null
                    else -> {
                        logger.info("Unsupported document was indexed in $INDEX_STATE_MANAGEMENT_INDEX")
                    }
                }
            }
            job
        }
    }

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): List<RestHandler> {
        return listOf(
            RestIndexPolicyAction(settings, restController, indexStateManagementIndices),
            RestGetPolicyAction(settings, restController),
            RestDeletePolicyAction(settings, restController)
        )
    }

    override fun createComponents(
        client: Client,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry
    ): Collection<Any> {
        val managedIndexRunner = ManagedIndexRunner.instance
        indexStateManagementIndices = IndexStateManagementIndices(client.admin().indices(), clusterService)
        val managedIndexCoordinator = ManagedIndexCoordinator(environment.settings(),
                client, clusterService, threadPool, indexStateManagementIndices)

        return listOf(managedIndexRunner, indexStateManagementIndices, managedIndexCoordinator)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.POLICY_NAME,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS
        )
    }
}
