package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportDeleteTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteTransformRequest, DeleteResponse>(
    DeleteTransformAction.NAME, transportService, actionFilters, ::DeleteTransformRequest
) {

    override fun doExecute(task: Task, request: DeleteTransformRequest, actionListener: ActionListener<DeleteResponse>) {
        request.index(INDEX_MANAGEMENT_INDEX)
        // TODO: get transport job and validate that job is disabled

        // TODO: if metadata id exists delete the metadata doc else just delete transform

        client.delete(request, object : ActionListener<DeleteResponse> {
            override fun onResponse(response: DeleteResponse) {
                actionListener.onResponse(response)
            }

            override fun onFailure(e: Exception?) {
                actionListener.onFailure(e)
            }
        })
    }
}
