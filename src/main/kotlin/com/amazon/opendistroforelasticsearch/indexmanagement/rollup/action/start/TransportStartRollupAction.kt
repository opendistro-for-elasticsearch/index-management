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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
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
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
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
        val getReq = GetRollupRequest(request.id(), null)
        client.execute(GetRollupAction.INSTANCE, getReq, object : ActionListener<GetRollupResponse> {
            override fun onResponse(response: GetRollupResponse) {
                val rollup = response.rollup
                if (rollup == null) {
                    return actionListener.onFailure(
                        ElasticsearchStatusException("Could not find rollup [${request.id()}]", RestStatus.NOT_FOUND)
                    )
                }

                if (rollup.enabled) {
                    log.debug("Rollup job is already enabled, checking if metadata needs to be updated")
                    return if (rollup.metadataID == null) {
                        actionListener.onResponse(AcknowledgedResponse(true))
                    } else {
                        getRollupMetadata(rollup, actionListener)
                    }
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
        request.index(INDEX_MANAGEMENT_INDEX).doc(mapOf(Rollup.ROLLUP_TYPE to mapOf(Rollup.ENABLED_FIELD to true,
                Rollup.ENABLED_TIME_FIELD to now, Rollup.LAST_UPDATED_TIME_FIELD to now)))
        client.update(request, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    // If there is a metadata ID on rollup then we need to set it back to STARTED or RETRY
                    if (rollup.metadataID != null) {
                        getRollupMetadata(rollup, actionListener)
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

    private fun getRollupMetadata(rollup: Rollup, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
        client.get(req, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists || response.isSourceEmpty) {
                    // If there is no metadata doc then the runner will instantiate a new one
                    // in FAILED status which the user will need to retry from
                    actionListener.onResponse(AcknowledgedResponse(true))
                } else {
                    val metadata = response.sourceAsBytesRef?.let {
                        val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON)
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                    }
                    if (metadata == null) {
                        // If there is no metadata doc then the runner will instantiate a new one
                        // in FAILED status which the user will need to retry from
                        actionListener.onResponse(AcknowledgedResponse(true))
                    } else {
                        updateRollupMetadata(rollup, metadata, actionListener)
                    }
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateRollupMetadata(rollup: Rollup, metadata: RollupMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            RollupMetadata.Status.FINISHED, RollupMetadata.Status.STOPPED -> RollupMetadata.Status.STARTED
            RollupMetadata.Status.STARTED, RollupMetadata.Status.INIT, RollupMetadata.Status.RETRY ->
                return actionListener.onResponse(AcknowledgedResponse(true))
            RollupMetadata.Status.FAILED -> RollupMetadata.Status.RETRY
        }
        val updateRequest = UpdateRequest(INDEX_MANAGEMENT_INDEX, rollup.metadataID)
            .doc(mapOf(RollupMetadata.ROLLUP_METADATA_TYPE to mapOf(RollupMetadata.STATUS_FIELD to updatedStatus.type,
                RollupMetadata.FAILURE_REASON to null, RollupMetadata.LAST_UPDATED_FIELD to now)))
            .routing(rollup.id)
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
