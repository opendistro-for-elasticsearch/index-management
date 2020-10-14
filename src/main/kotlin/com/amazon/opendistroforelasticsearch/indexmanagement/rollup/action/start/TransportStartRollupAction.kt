/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.time.Instant

class TransportStartRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StartRollupRequest, AcknowledgedResponse>(
    StartRollupAction.NAME, transportService, actionFilters, ::StartRollupRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val getReq = GetRollupRequest(request.id(), RestRequest.Method.GET, null)
        client.execute(GetRollupAction.INSTANCE, getReq, object : ActionListener<GetRollupResponse> {
            override fun onResponse(response: GetRollupResponse) {
                val rollup = response.rollup
                if (rollup == null) {
                    return actionListener.onFailure(
                        ElasticsearchStatusException("Could not find find rollup [${request.id()}]", RestStatus.NOT_FOUND)
                    )
                }

                if (rollup.enabled) {
                    log.debug("Rollup job is already enabled")
                    return actionListener.onResponse(AcknowledgedResponse(true))
                }

                updateRollupJob(rollup, request, actionListener)
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    // TODO: Should create a transport action to update metadata
    private fun updateRollupJob(rollup: Rollup, request: StartRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        request.index(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            .doc(mapOf(Rollup.ROLLUP_TYPE to mapOf(Rollup.ENABLED_FIELD to true,
                Rollup.ENABLED_TIME_FIELD to now, Rollup.LAST_UPDATED_TIME_FIELD to now)))
        client.update(request, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    // If there is a metadata ID on rollup then we need to set it back to STARTED
                    if (rollup.metadataID != null) {
                        updateRollupMetadata(rollup, actionListener)
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(true))
                    }
                } else {
                    actionListener.onResponse(AcknowledgedResponse(false))
                }
            }
            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateRollupMetadata(rollup: Rollup, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updateRequest = UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID)
            .doc(mapOf(RollupMetadata.ROLLUP_METADATA_TYPE to mapOf(RollupMetadata.STATUS_FIELD to RollupMetadata.Status.STARTED.type,
                RollupMetadata.FAILURE_REASON to null, RollupMetadata.LAST_UPDATED_FIELD to now)))
        client.update(updateRequest, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }
}
