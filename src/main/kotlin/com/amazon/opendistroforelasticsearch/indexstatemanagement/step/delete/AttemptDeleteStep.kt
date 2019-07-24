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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.delete

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.RetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import java.lang.Exception

class AttemptDeleteStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: DeleteActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    // TODO: Needs to return execute status after finishing, i.e. succeeded, noop, failed, failed info to update metadata
    override suspend fun execute() {
        try {
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { delete(DeleteIndexRequest(managedIndexMetaData.index), it) }

            if (response.isAcknowledged) {
                info = mapOf("message" to "Deleted index")
            } else {
                failed = true
                info = mapOf("message" to "Failed to delete index")
            }
        } catch (e: Exception) {
            failed = true
            val mutableInfo = mutableMapOf("message" to "Failed to delete index")
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo.put("cause", errorMessage)
            info = mutableInfo.toMap()
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            // TODO only update stepStartTime when first try of step and not retries
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), !failed),
            // TODO we should refactor such that transitionTo is not reset in the step.
            transitionTo = null,
            // TODO properly attempt retry and update RetryInfo.
            retryInfo = if (currentMetaData.retryInfo != null) currentMetaData.retryInfo.copy(failed = failed) else RetryInfoMetaData(failed, 0),
            info = info
        )
    }

    companion object {
        const val name = "attempt_delete"
    }
}
