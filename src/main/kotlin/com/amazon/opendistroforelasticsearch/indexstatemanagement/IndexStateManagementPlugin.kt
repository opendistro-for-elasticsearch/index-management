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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestAddPolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestDeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestExplainAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestGetPolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestIndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestRemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
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
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

internal class IndexStateManagementPlugin : JobSchedulerExtension, ActionPlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexStateManagementIndices: IndexStateManagementIndices
    lateinit var clusterService: ClusterService

    companion object {
        const val PLUGIN_NAME = "opendistro-ism"
        const val ISM_BASE_URI = "/_opendistro/_ism"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val INDEX_STATE_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_STATE_MANAGEMENT_JOB_TYPE = "opendistro-managed-index"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"
    }

    override fun getJobIndex(): String {
        return INDEX_STATE_MANAGEMENT_INDEX
    }

    override fun getJobType(): String {
        return INDEX_STATE_MANAGEMENT_JOB_TYPE
    }

    override fun getJobRunner(): ScheduledJobRunner {
        return ManagedIndexRunner
    }

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
                        logger.info("Unsupported document was indexed in $INDEX_STATE_MANAGEMENT_INDEX with type: $fieldName")
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
            RestIndexPolicyAction(settings, restController, indexStateManagementIndices),
            RestGetPolicyAction(settings, restController),
            RestDeletePolicyAction(settings, restController),
            RestExplainAction(settings, restController),
            RestRetryFailedManagedIndexAction(settings, restController),
            RestAddPolicyAction(settings, restController),
            RestRemovePolicyAction(settings, restController),
            RestChangePolicyAction(settings, restController, clusterService)
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
        val settings = environment.settings()
        this.clusterService = clusterService
        val managedIndexRunner = ManagedIndexRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)

        indexStateManagementIndices = IndexStateManagementIndices(settings, client.admin().indices(), threadPool, clusterService)
        val managedIndexCoordinator = ManagedIndexCoordinator(environment.settings(),
                client, clusterService, threadPool, indexStateManagementIndices)

        return listOf(managedIndexRunner, indexStateManagementIndices, managedIndexCoordinator)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.POLICY_ID,
            ManagedIndexSettings.ROLLOVER_ALIAS,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            ManagedIndexSettings.ISM_HISTORY_MAX_DOCS,
            ManagedIndexSettings.ISM_HISTORY_INDEX_MAX_AGE,
            ManagedIndexSettings.ISM_HISTORY_ROLLOVER_CHECK_PERIOD
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(
                UpdateManagedIndexMetaDataAction,
                TransportUpdateManagedIndexMetaDataAction::class.java
            )
        )
    }
}
