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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupRequest
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
import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.time.Instant

class TransportStopRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StopRollupRequest, AcknowledgedResponse>(
    StopRollupAction.NAME, transportService, actionFilters, ::StopRollupRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val getReq = GetRollupRequest(request.id(), Versions.MATCH_ANY, RestRequest.Method.GET, null)
        client.execute(GetRollupAction.INSTANCE, getReq, object : ActionListener<GetRollupResponse> {
            override fun onResponse(response: GetRollupResponse) {
                val rollup = response.rollup
                if (rollup == null) {
                    return actionListener.onFailure(
                        ElasticsearchStatusException("Could not find find rollup [${request.id()}]", RestStatus.NOT_FOUND)
                    )
                }

                // TODO: This check could be come stale if it's true and then immediately after the metadata is INIT in the runner
                if (rollup.metadataID != null) {
                    updateRollupMetadata(rollup, request, actionListener)
                } else {
                    updateRollupJob(request, actionListener)
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateRollupJob(request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        request.index(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            .doc(mapOf(Rollup.ROLLUP_TYPE to mapOf(Rollup.ENABLED_FIELD to false,
                Rollup.ENABLED_TIME_FIELD to null, Rollup.LAST_UPDATED_TIME_FIELD to now)))
        client.update(request, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
            }
            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateRollupMetadata(rollup: Rollup, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updateRequest = UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID)
            .doc(mapOf(RollupMetadata.ROLLUP_METADATA_TYPE to mapOf(RollupMetadata.STATUS_FIELD to RollupMetadata.Status.STOPPED.type,
                RollupMetadata.FAILURE_REASON to null, RollupMetadata.LAST_UPDATED_FIELD to now)))
        client.update(updateRequest, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    updateRollupJob(request, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(false))
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }
}
