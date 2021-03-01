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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.TransportAddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.TransportChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.TransportDeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPoliciesAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.TransportIndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.TransportRemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.TransportRetryFailedManagedIndexAction
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.RefreshSearchAnalyzerAction
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction
import com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer.TransportRefreshSearchAnalyzerAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupIndexer
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMapperService
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMetadataService
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupSearchService
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete.TransportDeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.TransportGetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.TransportIndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.TransportStartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.StopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.TransportStopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.TransportExplainRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.TransportGetRollupsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping.TransportUpdateRollupMappingAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter.FieldCapsFilter
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.interceptor.RollupInterceptor
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestDeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestExplainRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestGetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestIndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestStartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestStopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.support.ActionFilter
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
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.NetworkPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportInterceptor
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

internal class IndexManagementPlugin : JobSchedulerExtension, NetworkPlugin, ActionPlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexManagementIndices: IndexManagementIndices
    lateinit var clusterService: ClusterService
    lateinit var indexNameExpressionResolver: IndexNameExpressionResolver
    lateinit var rollupInterceptor: RollupInterceptor

    companion object {
        const val PLUGIN_NAME = "opendistro-im"
        const val OPEN_DISTRO_BASE_URI = "/_opendistro"
        const val ISM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_ism"
        const val ROLLUP_BASE_URI = "$OPEN_DISTRO_BASE_URI/_rollup"
        const val TRANSFORM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_transform"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val ROLLUP_JOBS_BASE_URI = "$ROLLUP_BASE_URI/jobs"
        const val INDEX_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_MANAGEMENT_JOB_TYPE = "opendistro-index-management"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"
    }

    override fun getJobIndex(): String = INDEX_MANAGEMENT_INDEX

    override fun getJobType(): String = INDEX_MANAGEMENT_JOB_TYPE

    override fun getJobRunner(): ScheduledJobRunner = IndexManagementRunner

    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, jobDocVersion ->
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
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
                    Rollup.ROLLUP_TYPE -> {
                        return@ScheduledJobParser Rollup.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    RollupMetadata.ROLLUP_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    else -> {
                        logger.warn("Unsupported document was indexed in $INDEX_MANAGEMENT_INDEX with type: $fieldName")
                        xcp.skipChildren()
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
            RestRefreshSearchAnalyzerAction(),
            RestIndexPolicyAction(settings, clusterService),
            RestGetPolicyAction(),
            RestDeletePolicyAction(),
            RestExplainAction(),
            RestRetryFailedManagedIndexAction(),
            RestAddPolicyAction(),
            RestRemovePolicyAction(),
            RestChangePolicyAction(),
            RestDeleteRollupAction(),
            RestGetRollupAction(),
            RestIndexRollupAction(),
            RestStartRollupAction(),
            RestStopRollupAction(),
            RestExplainRollupAction()
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
        val rollupRunner = RollupRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerThreadPool(threadPool)
            .registerMapperService(RollupMapperService(client, clusterService, indexNameExpressionResolver))
            .registerIndexer(RollupIndexer(settings, clusterService, client))
            .registerSearcher(RollupSearchService(settings, clusterService, client))
            .registerMetadataServices(RollupMetadataService(client, xContentRegistry))
            .registerConsumers()
        rollupInterceptor = RollupInterceptor(clusterService, settings, indexNameExpressionResolver)
        this.indexNameExpressionResolver = indexNameExpressionResolver
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

        return listOf(managedIndexRunner, rollupRunner, indexManagementIndices, managedIndexCoordinator, indexStateManagementHistory)
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
            ManagedIndexSettings.ALLOW_LIST,
            ManagedIndexSettings.SNAPSHOT_DENY_LIST,
            RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_INDEX,
            RollupSettings.ROLLUP_ENABLED,
            RollupSettings.ROLLUP_SEARCH_ENABLED
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(UpdateManagedIndexMetaDataAction.INSTANCE, TransportUpdateManagedIndexMetaDataAction::class.java),
            ActionPlugin.ActionHandler(RemovePolicyAction.INSTANCE, TransportRemovePolicyAction::class.java),
            ActionPlugin.ActionHandler(RefreshSearchAnalyzerAction.INSTANCE, TransportRefreshSearchAnalyzerAction::class.java),
            ActionPlugin.ActionHandler(AddPolicyAction.INSTANCE, TransportAddPolicyAction::class.java),
            ActionPlugin.ActionHandler(RetryFailedManagedIndexAction.INSTANCE, TransportRetryFailedManagedIndexAction::class.java),
            ActionPlugin.ActionHandler(ChangePolicyAction.INSTANCE, TransportChangePolicyAction::class.java),
            ActionPlugin.ActionHandler(IndexPolicyAction.INSTANCE, TransportIndexPolicyAction::class.java),
            ActionPlugin.ActionHandler(ExplainAction.INSTANCE, TransportExplainAction::class.java),
            ActionPlugin.ActionHandler(DeletePolicyAction.INSTANCE, TransportDeletePolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPolicyAction.INSTANCE, TransportGetPolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPoliciesAction.INSTANCE, TransportGetPoliciesAction::class.java),
            ActionPlugin.ActionHandler(DeleteRollupAction.INSTANCE, TransportDeleteRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupAction.INSTANCE, TransportGetRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupsAction.INSTANCE, TransportGetRollupsAction::class.java),
            ActionPlugin.ActionHandler(IndexRollupAction.INSTANCE, TransportIndexRollupAction::class.java),
            ActionPlugin.ActionHandler(StartRollupAction.INSTANCE, TransportStartRollupAction::class.java),
            ActionPlugin.ActionHandler(StopRollupAction.INSTANCE, TransportStopRollupAction::class.java),
            ActionPlugin.ActionHandler(ExplainRollupAction.INSTANCE, TransportExplainRollupAction::class.java),
            ActionPlugin.ActionHandler(UpdateRollupMappingAction.INSTANCE, TransportUpdateRollupMappingAction::class.java)
        )
    }

    override fun getTransportInterceptors(namedWriteableRegistry: NamedWriteableRegistry, threadContext: ThreadContext): List<TransportInterceptor> {
        return listOf(rollupInterceptor)
    }

    override fun getActionFilters(): List<ActionFilter> {
        return listOf(FieldCapsFilter(clusterService, indexNameExpressionResolver))
    }
}
