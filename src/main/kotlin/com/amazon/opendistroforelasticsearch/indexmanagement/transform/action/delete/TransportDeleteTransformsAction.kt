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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportDeleteTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteTransformsRequest, BulkResponse>(
    DeleteTransformsAction.NAME, transportService, actionFilters, ::DeleteTransformsRequest
) {

    override fun doExecute(task: Task, request: DeleteTransformsRequest, actionListener: ActionListener<BulkResponse>) {

        // TODO: if metadata id exists delete the metadata doc else just delete transform

        // Use Multi-Get Request
        val getRequest = MultiGetRequest()
        request.ids.forEach { id ->
            getRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, id))
        }

        client.multiGet(getRequest, object : ActionListener<MultiGetResponse> {
            override fun onResponse(response: MultiGetResponse) {
                try {
                    bulkDelete(response, request.ids, actionListener)
                } catch (e: Exception) {
                    actionListener.onFailure(e)
                }
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }

    private fun bulkDelete(response: MultiGetResponse, ids: List<String>, actionListener: ActionListener<BulkResponse>) {
        val enabledIDs = mutableListOf<String>()
        val sourceMissingIDs = mutableListOf<String>()
        val notTransform = mutableListOf<String>()

        response.responses.forEach {
            // TODO: Check if the source is actually transform document
            if (it.response != null && it.response.isExists) {
                if (it.response.isSourceEmpty) {
                    sourceMissingIDs.add(it.id)
                }
                val source = it.response.source
                if (!source.keys.contains("transform")) {
                    notTransform.add(it.id)
                }

                val transform = source["transform"] as Map<String, Any>
                val enabled = transform["enabled"] as Boolean
                if (enabled) {
                    enabledIDs.add(it.id)
                }
            }
        }

        if (notTransform.isNotEmpty()) {
            actionListener.onFailure(ElasticsearchStatusException(
                "[$notTransform] IDs are not transforms!", RestStatus.BAD_REQUEST
            ))
        }

        if (sourceMissingIDs.isNotEmpty()) {
            actionListener.onFailure(ElasticsearchStatusException(
                "[$sourceMissingIDs] are missing their source documents!", RestStatus.NOT_FOUND
            ))
        }

        if (enabledIDs.isNotEmpty()) {
            actionListener.onFailure(ElasticsearchStatusException(
                "[$enabledIDs] transform(s) are enabled, please disable them before deleting them", RestStatus.CONFLICT
            ))
        }

        val bulkDeleteRequest = BulkRequest()
        for (id in ids) {
            bulkDeleteRequest.add(DeleteRequest(INDEX_MANAGEMENT_INDEX, id))
        }

        client.bulk(bulkDeleteRequest, object : ActionListener<BulkResponse> {
            override fun onResponse(response: BulkResponse) {
                response.items.forEach {
                    if (it.failure != null && it.failure.cause::class == IndexNotFoundException::class) {
                        actionListener.onFailure(ElasticsearchStatusException("Index not found", RestStatus.NOT_FOUND))
                        return
                    }
                }
                actionListener.onResponse(response)
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }
}
