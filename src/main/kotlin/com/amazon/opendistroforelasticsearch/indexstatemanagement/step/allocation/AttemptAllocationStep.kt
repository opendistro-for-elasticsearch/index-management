package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.allocation

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.AllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings

class AttemptAllocationStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: AllocationActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_allocation", managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute() {
        try {
            val response: AcknowledgedResponse = client.admin()
                .indices()
                .suspendUntil { updateSettings(UpdateSettingsRequest(buildSettings(), managedIndexMetaData.index), it) }
            handleResponse(response)
        } catch (e: Exception) {
            logger.error(ERROR_MESSAGE, e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to ERROR_MESSAGE)
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }
    }

    private fun buildSettings(): Settings {
        val builder = Settings.builder()
        config.require.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.REQUIRE + "." + key, value) }
        config.include.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.INCLUDE + "." + key, value) }
        config.exclude.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationActionConfig.EXCLUDE + "." + key, value) }
        return builder.build()
    }

    private fun handleResponse(response: AcknowledgedResponse) {
        if (response.isAcknowledged) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to "Updated settings with allocation.")
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to ERROR_MESSAGE)
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        private const val ERROR_MESSAGE = "Failed to update settings with allocation."
        private const val SETTINGS_PREFIX = "index.routing.allocation."
    }
}
