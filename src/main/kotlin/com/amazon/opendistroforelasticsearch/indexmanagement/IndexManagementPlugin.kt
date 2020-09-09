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

package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestAddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestDeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestGetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestIndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.RefreshSynonymAnalyzerAction
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.RestRefreshSynonymAnalyzerAction
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.TransportRefreshSynonymAnalyzerAction
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
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
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

internal class IndexManagementPlugin : JobSchedulerExtension, ActionPlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexManagementIndices: IndexManagementIndices
    lateinit var clusterService: ClusterService

    companion object {
        const val PLUGIN_NAME = "opendistro-im"
        const val OPEN_DISTRO_BASE_URI = "/_opendistro"
        const val ISM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_ism"
        const val ANALYZER_BASE_URI = "$OPEN_DISTRO_BASE_URI/_analyzer"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val INDEX_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_MANAGEMENT_JOB_TYPE = "opendistro-index-management"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"
    }

    override fun getJobIndex(): String = INDEX_MANAGEMENT_INDEX

    override fun getJobType(): String = INDEX_MANAGEMENT_JOB_TYPE

    override fun getJobRunner(): ScheduledJobRunner = ManagedIndexRunner

    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, jobDocVersion ->
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.MANAGED_INDEX_TYPE -> {
                        return@ScheduledJobParser ManagedIndexConfig.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    Policy.POLICY_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    else -> {
                        logger.info("Unsupported document was indexed in $INDEX_MANAGEMENT_INDEX with type: $fieldName")
                    }
                }
            }
            return@ScheduledJobParser null
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
            RestRefreshSynonymAnalyzerAction(),
            RestIndexPolicyAction(settings, clusterService, indexManagementIndices),
            RestGetPolicyAction(),
            RestDeletePolicyAction(),
            RestExplainAction(),
            RestRetryFailedManagedIndexAction(),
            RestAddPolicyAction(),
            RestRemovePolicyAction(),
            RestChangePolicyAction(clusterService)
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
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>
    ): Collection<Any> {
        val settings = environment.settings()
        this.clusterService = clusterService
        val managedIndexRunner = ManagedIndexRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerConsumers() // registerConsumers must happen after registerSettings/clusterService

        indexManagementIndices = IndexManagementIndices(client.admin().indices(), clusterService)
        val indexStateManagementHistory =
            IndexStateManagementHistory(
                settings,
                client,
                threadPool,
                clusterService,
                indexManagementIndices
            )

        val managedIndexCoordinator = ManagedIndexCoordinator(environment.settings(),
            client, clusterService, threadPool, indexManagementIndices)

        return listOf(managedIndexRunner, indexManagementIndices, managedIndexCoordinator, indexStateManagementHistory)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.HISTORY_ENABLED,
            ManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            ManagedIndexSettings.HISTORY_MAX_DOCS,
            ManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            ManagedIndexSettings.POLICY_ID,
            ManagedIndexSettings.ROLLOVER_ALIAS,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.JOB_INTERVAL,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            ManagedIndexSettings.ALLOW_LIST
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(
                UpdateManagedIndexMetaDataAction.INSTANCE,
                TransportUpdateManagedIndexMetaDataAction::class.java
            ),
            ActionPlugin.ActionHandler(
                RefreshSynonymAnalyzerAction.INSTANCE,
                TransportRefreshSynonymAnalyzerAction::class.java
            )
        )
    }
}
