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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformSearchService
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportPreviewTransformAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val esClient: Client
) : HandledTransportAction<PreviewTransformRequest, PreviewTransformResponse>(
    PreviewTransformAction.NAME, transportService, actionFilters, ::PreviewTransformRequest
) {

    override fun doExecute(task: Task, request: PreviewTransformRequest, listener: ActionListener<PreviewTransformResponse>) {
        val transform = request.transform
        val searchRequest = TransformSearchService.getSearchServiceRequest(transform = transform, pageSize = 10)
        esClient.search(searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                try {
                    val transformSearchResult = TransformSearchService.convertResponse(
                        transform = transform, searchResponse = response, waterMarkDocuments = false
                    )
                    val formattedResult = transformSearchResult.docsToIndex.map {
                        it.sourceAsMap()
                    }
                    listener.onResponse(PreviewTransformResponse(formattedResult, RestStatus.OK))
                } catch (e: Exception) {
                    listener.onFailure(ElasticsearchStatusException(
                        "Failed to parse the transformed results", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e))
                    )
                }
            }

            override fun onFailure(e: Exception) = listener.onFailure(e)
        })
    }
}
