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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.start

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
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

class TransportStartTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StartTransformRequest, AcknowledgedResponse>(
    StartTransformAction.NAME, transportService, actionFilters, ::StartTransformRequest
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val getReq = GetTransformRequest(request.id())
        client.execute(GetTransformAction.INSTANCE, getReq, object : ActionListener<GetTransformResponse> {
            override fun onResponse(response: GetTransformResponse) {
                val transform = response.transform
                if (transform == null) {
                    return actionListener.onFailure(
                        ElasticsearchStatusException("Could not find transform [${request.id()}]",
                            RestStatus.NOT_FOUND)
                    )
                }

                if (transform.enabled) {
                    log.debug("Transform job is already enabled, checking if metadata needs to be updated")
                    return if (transform.metadataId == null) {
                        actionListener.onResponse(AcknowledgedResponse(true))
                    } else {
                        retrieveAndUpdateTransformMetadata(transform, actionListener)
                    }
                }

                updateTransformJob(transform, request, actionListener)
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateTransformJob(
        transform: Transform,
        request: StartTransformRequest,
        actionListener: ActionListener<AcknowledgedResponse>
    ) {
        val now = Instant.now().toEpochMilli()
        request.index(INDEX_MANAGEMENT_INDEX).doc(mapOf(Transform.TRANSFORM_TYPE to mapOf(Transform.ENABLED_FIELD to true,
            Transform.ENABLED_AT_FIELD to now, Transform.UPDATED_AT_FIELD to now)))
        client.update(request, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                if (response.result == DocWriteResponse.Result.UPDATED) {
                    // If there is a metadata ID on transform then we need to set it back to STARTED or RETRY
                    if (transform.metadataId != null) {
                        retrieveAndUpdateTransformMetadata(transform, actionListener)
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

    private fun retrieveAndUpdateTransformMetadata(transform: Transform, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
        client.get(req, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists || response.isSourceEmpty) {
                    // If there is no metadata doc then the runner will instantiate a new one
                    // in FAILED status which the user will need to retry from
                    actionListener.onFailure(ElasticsearchStatusException("Metadata doc missing for transform [${req.id()}]",
                        RestStatus.NOT_FOUND))
                } else {
                    val metadata = response.sourceAsBytesRef?.let {
                        val xcp = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON)
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                    }
                    if (metadata == null) {
                        // If there is no metadata doc then the runner will instantiate a new one
                        // in FAILED status which the user will need to retry from
                        actionListener.onFailure(ElasticsearchStatusException("Metadata doc missing for transform [${req.id()}]",
                            RestStatus.NOT_FOUND))
                    } else {
                        updateTransformMetadata(transform, metadata, actionListener)
                    }
                }
            }

            override fun onFailure(e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        })
    }

    private fun updateTransformMetadata(transform: Transform, metadata: TransformMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus = when (metadata.status) {
            TransformMetadata.Status.FINISHED, TransformMetadata.Status.STOPPED -> TransformMetadata.Status.STARTED
            TransformMetadata.Status.STARTED, TransformMetadata.Status.INIT ->
                return actionListener.onResponse(AcknowledgedResponse(true))
            TransformMetadata.Status.FAILED -> TransformMetadata.Status.STARTED
        }
        val updateRequest = UpdateRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId)
            .doc(mapOf(TransformMetadata.TRANSFORM_METADATA_TYPE to mapOf(TransformMetadata.STATUS_FIELD to updatedStatus.type,
                TransformMetadata.FAILURE_REASON to null, TransformMetadata.LAST_UPDATED_AT_FIELD to now)))
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
