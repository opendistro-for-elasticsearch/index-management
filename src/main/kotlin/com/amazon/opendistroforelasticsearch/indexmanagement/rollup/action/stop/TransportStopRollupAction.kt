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

/**
 * Disables a rollup job and updates the rollup metadata if required.
 *
 * Stopping a rollup job requires up to two calls to be done.
 * 1. Disable the job itself so it stops being scheduled and executed by job scheduler.
 * 2. Update the rollup metadata status to reflect that it is not running anymore.
 *
 * There are no transactions so we will attempt to do the calls serially with the second relying
 * on the first ones success. With that in mind it's better to update metadata first and rollup job second
 * as a metadata: successful and job: failed can be recovered from in the runner where it will disable the job.
 * The inverse (job: successful and metadata: fail) will end up with a disabled job and a metadata that potentially
 * says STARTED still which is wrong.
 */
class TransportStopRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StopRollupRequest, AcknowledgedResponse>(
    StopRollupAction.NAME, transportService, actionFilters, ::StopRollupRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug("Executing StopRollupAction on ${request.id()}")
        val getReq = GetRollupRequest(request.id(), null)
        client.threadPool().threadContext.stashContext().use {
            client.execute(GetRollupAction.INSTANCE, getReq, object : ActionListener<GetRollupResponse> {
                override fun onResponse(response: GetRollupResponse) {
                    val rollup = response.rollup
                    if (rollup == null) {
                        return actionListener.onFailure(
                            ElasticsearchStatusException(
                                "Could not find rollup [${request.id()}]",
                                RestStatus.NOT_FOUND
                            )
                        )
                    }

                    if (rollup.metadataID != null) {
                        getRollupMetadata(rollup, request, actionListener)
                    } else {
                        updateRollupJob(rollup, request, actionListener)
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            })
        }
    }

    private fun getRollupMetadata(rollup: Rollup, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
        client.get(req, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists || response.isSourceEmpty) {
                    // If there is no metadata there is nothing to stop, proceed to disable job
                    updateRollupJob(rollup, request, actionListener)
                } else {
                    val metadata = response.sourceAsBytesRef?.let {
                        val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON)
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                    }
                    if (metadata == null) {
                        // If there is no metadata there is nothing to stop, proceed to disable job
                        updateRollupJob(rollup, request, actionListener)
                    } else {
                        updateRollupMetadata(rollup, metadata, request, actionListener)
                    }
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    /**
     * Updates the rollup metadata if required.
     *
     * The update is dependent on what the current [RollupMetadata.status] is.
     * When stopping a rollup that is in INIT, STARTED, or STOPPED we will update to STOPPED.
     * When the rollup is in FINISHED or FAILED it will remain as that status.
     * When the rollup is in RETRY we will update it back to a FAILED status so that it can correctly go back into RETRY if needed.
     */
    private fun updateRollupMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        request: StopRollupRequest,
        actionListener: ActionListener<AcknowledgedResponse>
    ) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            RollupMetadata.Status.STARTED, RollupMetadata.Status.INIT, RollupMetadata.Status.STOPPED -> RollupMetadata.Status.STOPPED
            RollupMetadata.Status.FINISHED, RollupMetadata.Status.FAILED -> metadata.status
            RollupMetadata.Status.RETRY -> RollupMetadata.Status.FAILED
        }
        val failureReason = if (metadata.status == RollupMetadata.Status.RETRY)
            "Stopped a rollup that was in retry, rolling back to failed status" else null
        val updateRequest = UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID)
            .doc(mapOf(RollupMetadata.ROLLUP_METADATA_TYPE to mapOf(RollupMetadata.STATUS_FIELD to updatedStatus.type,
                RollupMetadata.FAILURE_REASON to failureReason, RollupMetadata.LAST_UPDATED_FIELD to now)))
        client.update(updateRequest, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    updateRollupJob(rollup, request, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(false))
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateRollupJob(rollup: Rollup, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        request.index(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).setIfSeqNo(rollup.seqNo).setIfPrimaryTerm(rollup.primaryTerm)
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
}
