package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.close

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AttemptCloseStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: CloseActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_close", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed = false
    private var info: Map<String, Any>? = null

    override suspend fun execute() {
        logger.info("Executing close on ${managedIndexMetaData.index}")
        val closeIndexRequest = CloseIndexRequest()
            .indices(managedIndexMetaData.index)

        val response: AcknowledgedResponse = client.admin().indices().suspendUntil { close(closeIndexRequest, it) }
        if (!response.isAcknowledged) {
            failed = true
            info = mapOf("message" to "Failed to close index: ${managedIndexMetaData.index}")
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            step = name,
            stepStartTime = getStepStartTime().toEpochMilli(),
            transitionTo = null,
            stepCompleted = !failed,
            failed = failed,
            info = info
        )
    }
}
