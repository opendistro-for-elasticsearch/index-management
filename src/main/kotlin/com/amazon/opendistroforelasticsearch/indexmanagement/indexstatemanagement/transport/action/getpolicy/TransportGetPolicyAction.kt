package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportGetPolicyAction @Inject constructor(
        val client: NodeClient,
        transportService: TransportService,
        actionFilters: ActionFilters,
        val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPolicyRequest, GetPolicyResponse>(
        GetPolicyAction.NAME, transportService, actionFilters, ::GetPolicyRequest
) {
    override fun doExecute(task: Task, request: GetPolicyRequest, listener: ActionListener<GetPolicyResponse>) {
        GetPolicyHandler(client, listener, request).start()
    }

    inner class GetPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetPolicyResponse>,
        private val request: GetPolicyRequest
    ) {
        fun start() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .version(request.version)
                .fetchSourceContext(request.fetchSrcContext)

            client.get(getRequest, object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    onGetResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(t)
                }
            })
        }

        fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(ElasticsearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }

            var policy: Policy? = null
            if (!response.isSourceEmpty) {
                XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef,
                    XContentType.JSON
                ).use { xcp ->
                    policy = Policy.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
                }
            }

            actionListener.onResponse(
                GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, policy)
            )
        }
    }
}