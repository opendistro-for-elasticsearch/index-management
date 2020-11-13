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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.notification

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.convertToMap
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.NotificationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService
import org.elasticsearch.script.TemplateScript

class AttemptNotificationStep(
    val clusterService: ClusterService,
    val scriptService: ScriptService,
    val client: Client,
    val config: NotificationActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_notification", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptNotificationStep {
        try {
            withContext(Dispatchers.IO) {
                config.destination.publish(null, compileTemplate(config.messageTemplate, managedIndexMetaData))
            }

            // publish internally throws an error for any invalid responses so its safe to assume if we reach this point it was successful
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    private fun compileTemplate(template: Script, managedIndexMetaData: ManagedIndexMetaData): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to managedIndexMetaData.convertToMap()))
            .execute()
    }

    companion object {
        fun getFailedMessage(index: String) = "Failed to send notification [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully sent notification [index=$index]"
    }
}
