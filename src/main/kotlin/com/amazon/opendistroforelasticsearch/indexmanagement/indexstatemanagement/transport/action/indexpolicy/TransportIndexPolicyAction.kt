/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.filterNotNullValues
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.getPolicyToTemplateMap
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.findConflictingPolicyTemplates
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.findSelfConflictingTemplates
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexManagementException
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.ISM_TEMPLATES_FIELD
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.validateFormat
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import org.apache.logging.log4j.LogManager
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportIndexPolicyAction::class.java)

class TransportIndexPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val ismIndices: IndexManagementIndices,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexPolicyRequest, IndexPolicyResponse>(
        IndexPolicyAction.NAME, transportService, actionFilters, ::IndexPolicyRequest
) {
    override fun doExecute(task: Task, request: IndexPolicyRequest, listener: ActionListener<IndexPolicyResponse>) {
        IndexPolicyHandler(client, listener, request).start()
    }

    inner class IndexPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<IndexPolicyResponse>,
        private val request: IndexPolicyRequest
    ) {
        fun start() {
            ismIndices.checkAndUpdateIMConfigIndex(object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(response: AcknowledgedResponse) {
                    onCreateMappingsResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mappings.")

                // if there is template field, we will check
                val reqTemplates = request.policy.ismTemplates
                if (reqTemplates != null) {
                    validateISMTemplates(reqTemplates)
                } else putPolicy()
            } else {
                log.error("Unable to create or update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mapping.")

                actionListener.onFailure(ElasticsearchStatusException(
                    "Unable to create or update ${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX} with newest mapping.",
                    RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun validateISMTemplates(ismTemplateList: List<ISMTemplate>) {
            val possibleEx = validateFormat(ismTemplateList.map { it.indexPatterns }.flatten())
            if (possibleEx != null) {
                actionListener.onFailure(possibleEx)
                return
            }

            // check self overlapping
            val selfOverlap = ismTemplateList.findSelfConflictingTemplates()
            if (selfOverlap != null) {
                val errorMessage = "New policy ${request.policyID} has an ISM template with index pattern ${selfOverlap.first} " +
                    "matching this policy's other ISM templates with index patterns ${selfOverlap.second}," +
                    " please use different priority"
                actionListener.onFailure(IndexManagementException.wrap(IllegalArgumentException(errorMessage)))
                return
            }

            val searchRequest = SearchRequest()
                .source(
                    SearchSourceBuilder().query(
                        QueryBuilders.nestedQuery(
                            ISM_TEMPLATES_FIELD,
                            QueryBuilders.existsQuery(ISM_TEMPLATES_FIELD),
                            ScoreMode.None
                        )
                    ).size(MAX_HITS)
                )
                .indices(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)

            client.search(searchRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val policyToTemplateMap = getPolicyToTemplateMap(response, xContentRegistry).filterNotNullValues()
                    ismTemplateList.forEach {
                        val conflictingPolicyTemplates = policyToTemplateMap
                            .findConflictingPolicyTemplates(request.policyID, it.indexPatterns, it.priority)
                        if (conflictingPolicyTemplates.isNotEmpty()) {
                            val errorMessage = "New policy ${request.policyID} has an ISM template with index pattern ${it.indexPatterns} " +
                                "matching existing policy templates," +
                                " please use a different priority than ${it.priority}"
                            actionListener.onFailure(IndexManagementException.wrap(IllegalArgumentException(errorMessage)))
                            return
                        }
                    }

                    putPolicy()
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        private fun putPolicy() {
            request.policy.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion)

            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(request.policy.toXContent(XContentFactory.jsonBuilder()))
                .id(request.policyID)
                .timeout(IndexRequest.DEFAULT_TIMEOUT)

            if (request.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || request.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            } else {
                indexRequest.setIfSeqNo(request.seqNo)
                        .setIfPrimaryTerm(request.primaryTerm)
            }

            client.index(indexRequest, object : ActionListener<IndexResponse> {
                override fun onResponse(response: IndexResponse) {
                    val failureReasons = checkShardsFailure(response)
                    if (failureReasons != null) {
                        actionListener.onFailure(ElasticsearchStatusException(failureReasons.toString(), response.status()))
                        return
                    }
                    actionListener.onResponse(IndexPolicyResponse(
                        response.id,
                        response.version,
                        response.primaryTerm,
                        response.seqNo,
                        request.policy,
                        response.status()
                    ))
                }

                override fun onFailure(t: Exception) {
                    // TODO should wrap document already exists exception
                    //  provide a direct message asking user to use seqNo and primaryTerm
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach {
                    entry -> failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }
}
