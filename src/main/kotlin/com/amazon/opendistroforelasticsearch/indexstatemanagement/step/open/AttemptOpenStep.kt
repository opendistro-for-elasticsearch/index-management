package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.open

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AttemptOpenStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: OpenActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_open", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed = false
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        try {
            logger.info("Executing open on ${managedIndexMetaData.index}")
            val openIndexRequest = OpenIndexRequest()
                .indices(managedIndexMetaData.index)

            val response: OpenIndexResponse = client.admin().indices().suspendUntil { open(openIndexRequest, it) }
            if (!response.isAcknowledged) {
                failed = true
                info = mapOf("message" to "Failed to open index: ${managedIndexMetaData.index}")
            } else {
                info = mapOf("message" to "Successfully opened index")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to set index to open")
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
