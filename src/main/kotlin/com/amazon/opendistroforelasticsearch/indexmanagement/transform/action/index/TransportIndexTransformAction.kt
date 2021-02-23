package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.Exception

class TransportIndexTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService
) : HandledTransportAction<IndexTransformRequest, IndexTransformResponse>(
        IndexTransformAction.NAME, transportService, actionFilters, ::IndexTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexTransformRequest, listener: ActionListener<IndexTransformResponse>) {
        IndexTransformHandler(client, listener, request).start()
    }

    inner class IndexTransformHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexTransformResponse>,
        private val request: IndexTransformRequest
    ) {

        fun start() {
            indexManagementIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onCreateTransformResponse, actionListener::onFailure))
        }

        private fun onCreateTransformResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest transform.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    putTransform()
                } else {
                    getTransform()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest transform."
                log.error(message)
                actionListener.onFailure(ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun getTransform() {
            val getReq = GetTransformRequest(request.transform.id, null)
            client.execute(GetTransformAction.INSTANCE, getReq, ActionListener.wrap(::onGetTransform, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetTransform(response: GetTransformResponse) {
            if (response.status != RestStatus.OK) {
                return actionListener.onFailure(ElasticsearchStatusException("Unable to get existing transform", response.status))
            }
            val transform = response.transform
                ?: return actionListener.onFailure(ElasticsearchStatusException("The current transform is null", RestStatus.INTERNAL_SERVER_ERROR))
            val modified = modifiedImmutableProperties(transform, request.transform)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(ElasticsearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            putTransform()
        }

        private fun modifiedImmutableProperties(transform: Transform, newTransform: Transform): List<String> {
            val modified = mutableListOf<String>()
            if (transform.sourceIndex != newTransform.sourceIndex) modified.add(Transform.SOURCE_INDEX_FIELD)
            if (transform.targetIndex != newTransform.targetIndex) modified.add(Transform.TARGET_INDEX_FIELD)
            if (transform.dataSelectionQuery != newTransform.dataSelectionQuery) modified.add(Transform.DATA_SELECTION_QUERY_FIELD)
            if (transform.groups != newTransform.groups) modified.add(Transform.GROUPS_FIELD)
            if (transform.aggregations != newTransform.aggregations) modified.add(Transform.AGGREGATIONS_FIELD)
            if (transform.roles != newTransform.roles) modified.add(Transform.ROLES_FIELD)
            return modified.toList()
        }

        private fun putTransform() {
            val transform = request.transform.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.transform.id)
                .source(transform.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(request, object : ActionListener<IndexResponse> {
                override fun onResponse(response: IndexResponse) {
                    if (response.shardInfo.failed > 0) {
                        val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                        actionListener.onFailure(ElasticsearchStatusException(failureReasons, response.status()))
                    } else {
                        val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                        actionListener.onResponse(
                                IndexTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, status,
                                        transform.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm))
                        )
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(e)
                }
            })
        }
    }
}
