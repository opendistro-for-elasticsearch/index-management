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
        var mgetRequest = MultiGetRequest()
        for (id in request.ids) {
            mgetRequest.add(MultiGetRequest.Item(
                INDEX_MANAGEMENT_INDEX,
                id
            ))
        }

        client.multiGet(mgetRequest, object : ActionListener<MultiGetResponse> {
            override fun onResponse(response: MultiGetResponse) {
                checkEnabled(response, actionListener)

                var bulkDeleteRequest = BulkRequest()
                for (response in response.responses) {
                    bulkDeleteRequest.add(DeleteRequest(INDEX_MANAGEMENT_INDEX, response.id))
                }
                bulkDelete(bulkDeleteRequest, actionListener)
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }

    private fun checkEnabled(response: MultiGetResponse, actionListener: ActionListener<BulkResponse>) {
        var enabledIDs = mutableListOf<String>()
        for (getResponse in response.responses) {
            var itemResponse = getResponse.response
            if ((itemResponse.getField("enabled").getValue() as Boolean)) {
                // add to list of enabled, don't fail
                enabledIDs.add(getResponse.id)
            }
        }
        if (enabledIDs.isNotEmpty()) {
            actionListener.onFailure(ElasticsearchStatusException("The following transform(s) are enabled. Please disable them before deleting: $enabledIDs", RestStatus.CONFLICT))
        }
    }

    private fun bulkDelete(bulkDeleteRequest: BulkRequest, actionListener: ActionListener<BulkResponse>) {
        client.bulk(bulkDeleteRequest, object : ActionListener<BulkResponse> {
            override fun onResponse(response: BulkResponse) {
                actionListener.onResponse(response)
            }

            override fun onFailure(e: Exception) = actionListener.onFailure(e)
        })
    }
}
