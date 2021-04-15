/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.stop

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
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
 * Disables a transform job and updates the transform metadata if required.
 *
 * Stopping a transform job requires up to two calls to be done.
 * 1. Disable the job itself so it stops being scheduled and executed by job scheduler.
 * 2. Update the transform metadata status to reflect that it is not running anymore.
 *
 * There are no transactions so we will attempt to do the calls serially with the second relying
 * on the first ones success. With that in mind it's better to update metadata first and transform job second
 * as a metadata: successful and job: failed can be recovered from in the runner where it will disable the job.
 * The inverse (job: successful and metadata: fail) will end up with a disabled job and a metadata that potentially
 * says STARTED still which is wrong.
 */
class TransportStopTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StopTransformRequest, AcknowledgedResponse>(
    StopTransformAction.NAME, transportService, actionFilters, ::StopTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StopTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug("Executing StopTransformAction on ${request.id()}")
        val getReq = GetTransformRequest(request.id(), null)
        client.execute(GetTransformAction.INSTANCE, getReq, object : ActionListener<GetTransformResponse> {
            override fun onResponse(response: GetTransformResponse) {
                val transform = response.transform
                if (transform == null) {
                    return actionListener.onFailure(
                        ElasticsearchStatusException("Could not find transform [${request.id()}]", RestStatus.NOT_FOUND)
                    )
                }

                if (transform.metadataId != null) {
                    retrieveAndUpdateTransformMetadata(transform, request, actionListener)
                } else {
                    updateTransformJob(transform, request, actionListener)
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun retrieveAndUpdateTransformMetadata(transform: Transform, request: StopTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
        client.get(req, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists || response.isSourceEmpty) {
                    // If there is no metadata there is nothing to stop, proceed to disable job
                    updateTransformJob(transform, request, actionListener)
                } else {
                    val metadata = response.sourceAsBytesRef?.let {
                        val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON)
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                    }
                    if (metadata == null) {
                        // If there is no metadata there is nothing to stop, proceed to disable job
                        updateTransformJob(transform, request, actionListener)
                    } else {
                        updateTransformMetadata(transform, metadata, request, actionListener)
                    }
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    /**
     * Updates the transform metadata if required.
     *
     * The update is dependent on what the current [TransformMetadata.status] is.
     * When stopping a transform that is in INIT, STARTED, or STOPPED we will update to STOPPED.
     * When the transform is in FINISHED or FAILED it will remain as that status.
     */
    private fun updateTransformMetadata(
        transform: Transform,
        metadata: TransformMetadata,
        request: StopTransformRequest,
        actionListener: ActionListener<AcknowledgedResponse>
    ) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            TransformMetadata.Status.STARTED, TransformMetadata.Status.INIT, TransformMetadata.Status.STOPPED -> TransformMetadata.Status.STOPPED
            TransformMetadata.Status.FINISHED, TransformMetadata.Status.FAILED -> metadata.status
        }

        val updateRequest = UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, transform.metadataId)
            .doc(mapOf(TransformMetadata.TRANSFORM_METADATA_TYPE to mapOf(TransformMetadata.STATUS_FIELD to updatedStatus.type,
                TransformMetadata.LAST_UPDATED_AT_FIELD to now)))
        client.update(updateRequest, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    updateTransformJob(transform, request, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(false))
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateTransformJob(transform: Transform, request: StopTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        request.index(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).setIfSeqNo(transform.seqNo).setIfPrimaryTerm(transform.primaryTerm)
            .doc(mapOf(Transform.TRANSFORM_TYPE to mapOf(Transform.ENABLED_FIELD to false,
                Transform.ENABLED_AT_FIELD to null, Transform.UPDATED_AT_FIELD to now)))
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
