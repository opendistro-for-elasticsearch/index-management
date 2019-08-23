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
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute() {
        try {
            logger.info("Executing open on ${managedIndexMetaData.index}")
            val openIndexRequest = OpenIndexRequest()
                .indices(managedIndexMetaData.index)

            val response: OpenIndexResponse = client.admin().indices().suspendUntil { open(openIndexRequest, it) }
            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to "Successfully opened index")
            } else {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to "Failed to open index: ${managedIndexMetaData.index}")
            }
        } catch (e: Exception) {
            logger.error("Failed to set index to open [index=${managedIndexMetaData.index}]", e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to "Failed to set index to open")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }
}
