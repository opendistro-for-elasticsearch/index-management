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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ExplainRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.IdsQueryBuilder
import org.elasticsearch.index.query.QueryStringQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.transport.TransportService
import kotlin.Exception

class TransportExplainRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<ExplainRollupRequest, ExplainRollupResponse>(
    ExplainRollupAction.NAME, transportService, actionFilters, ::ExplainRollupRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainRollupRequest, actionListener: ActionListener<ExplainRollupResponse>) {
        val ids = request.rollupIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainRollup?> = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
        // First search is for all rollup documents that match at least one of the given rollupIDs
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
            .source(SearchSourceBuilder().query(
                BoolQueryBuilder().minimumShouldMatch(1).apply {
                    ids.forEach {
                        this.should(QueryStringQueryBuilder("*$it*").defaultField("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}"))
                    }
                }
            ))
        client.search(searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                try {
                    response.hits.hits.forEach {
                        val rollup = Rollup.parseWithType(contentParser(it.sourceRef), it.id, it.seqNo, it.primaryTerm)
                        idsToExplain[rollup.id] = ExplainRollup(metadataID = rollup.metadataID)
                    }
                } catch (e: Exception) {
                    log.error("Failed to parse explain response", e)
                    actionListener.onFailure(e)
                    return
                }

                val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                    .source(SearchSourceBuilder().query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                client.search(metadataSearchRequest, object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        try {
                            response.hits.hits.forEach {
                                val metadata = RollupMetadata.parseWithType(contentParser(it.sourceRef), it.id, it.seqNo, it.primaryTerm)
                                idsToExplain.computeIfPresent(metadata.rollupID) { _, explainRollup -> explainRollup.copy(metadata = metadata) }
                            }
                            actionListener.onResponse(ExplainRollupResponse(idsToExplain.toMap()))
                        } catch (e: Exception) {
                            log.error("Failed to parse rollup metadata", e)
                            actionListener.onFailure(e)
                            return
                        }
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search rollup metadata", e)
                        when (e) {
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                })
            }

            override fun onFailure(e: Exception) {
                log.error("Failed to search for rollups", e)
                when (e) {
                    is ResourceNotFoundException -> {
                        val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                        actionListener.onResponse(ExplainRollupResponse(nonWildcardIds))
                    }
                    is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    else -> actionListener.onFailure(e)
                }
            }
        })
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
    }
}
