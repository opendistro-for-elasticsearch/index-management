package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.close

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
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

    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        try {
            logger.info("Executing close on ${managedIndexMetaData.index}")
            val closeIndexRequest = CloseIndexRequest()
                .indices(managedIndexMetaData.index)

            val response: AcknowledgedResponse = client.admin().indices().suspendUntil { close(closeIndexRequest, it) }
            if (!response.isAcknowledged) {
                failed = true
                info = mapOf("message" to "Failed to close index: ${managedIndexMetaData.index}")
            } else {
                info = mapOf("message" to "Successfully closed index")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to set index to close")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), !failed),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            info = info
        )
    }
}
