package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getPolicyName
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.index.Index
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestActionListener
import org.elasticsearch.rest.action.RestResponseListener

class RestRetryFailedManagedIndexAction(
    settings: Settings,
    controller: RestController
) : BaseRestHandler(settings) {

    private val log = LogManager.getLogger(javaClass)

    init {
        controller.registerHandler(RestRequest.Method.POST, RETRY_BASE_URI, this)
        controller.registerHandler(RestRequest.Method.POST, "$RETRY_BASE_URI/{index}", this)
    }

    override fun getName(): String {
        return "retry_failed_managed_index"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }
        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val strictExpandIndicesOptions = IndicesOptions.strictExpand()

        val clusterStateRequest = ClusterStateRequest()
        clusterStateRequest.clear()
            .indices(*indices)
            .metaData(true)
            .masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()))
            .indicesOptions(strictExpandIndicesOptions)

        return RestChannelConsumer { client.admin().cluster().state(clusterStateRequest, IndexDestinationHandler(client, it, body["state"] as String?)) }
    }

    inner class IndexDestinationHandler(
        private val client: NodeClient,
        channel: RestChannel,
        private val startState: String?
    ) : RestActionListener<ClusterStateResponse>(channel) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfIndexMetaData: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()

        override fun processResponse(clusterStateResponse: ClusterStateResponse) {
            val state = clusterStateResponse.state
            populateList(state, startState)

            val builder = channel.newBuilder().startObject()
            if (listOfIndexMetaData.isNotEmpty()) {
                val updateManagedIndexMetaDataRequest = UpdateManagedIndexMetaDataRequest(listOfIndexMetaData)

                try {
                    client.execute(UpdateManagedIndexMetaDataAction, updateManagedIndexMetaDataRequest,
                        object : RestResponseListener<AcknowledgedResponse>(channel) {
                            override fun buildResponse(acknowledgedResponse: AcknowledgedResponse): RestResponse {
                                if (acknowledgedResponse.isAcknowledged) {
                                    builder.field(UPDATED_INDICES, listOfIndexMetaData.size)
                                } else {
                                    failedIndices.addAll(listOfIndexMetaData.map {
                                        FailedIndex(it.first.name, it.first.uuid, "failed to update IndexMetaData")
                                    })
                                }
                                buildInvalidIndexResponse(builder)
                                return BytesRestResponse(RestStatus.OK, builder.endObject())
                            }
                        }
                    )
                } catch (e: ClusterBlockException) {
                    failedIndices.addAll(listOfIndexMetaData.map {
                        FailedIndex(it.first.name, it.first.uuid, "failed to update with ClusterBlockException. ${e.message}")
                    })
                    buildInvalidIndexResponse(builder)
                    channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
                }
            } else {
                buildInvalidIndexResponse(builder)
                channel.sendResponse(BytesRestResponse(RestStatus.OK, builder.endObject()))
            }
        }

        private fun buildInvalidIndexResponse(builder: XContentBuilder) {
            if (failedIndices.isNotEmpty()) {
                builder.field(FAILURES, true)
                builder.startArray(FAILED_INDICES)
                for (failedIndex in failedIndices) {
                    builder.startObject()
                    builder.field("index_name", failedIndex.name)
                    builder.field("index_uuid", failedIndex.uuid)
                    builder.field("reason", failedIndex.reason)
                    builder.endObject()
                }
                builder.endArray()
            } else {
                builder.field(FAILURES, false)
            }
        }

        private fun populateList(state: ClusterState, startState: String?) {
            for (indexMetadataEntry in state.metaData.indices) {
                val indexMetadata = indexMetadataEntry.value

                if (indexMetadata.getPolicyName() == null) {
                    failedIndices.add(FailedIndex(indexMetadata.index.name, indexMetadata.index.uuid, "This index is not being managed."))
                    continue
                }

                val managedIndexMetaData = indexMetadata.getManagedIndexMetaData()
                if (managedIndexMetaData == null) {
                    failedIndices.add(FailedIndex(indexMetadata.index.name, indexMetadata.index.uuid, "There is no IndexMetaData information"))
                } else {
                    if (managedIndexMetaData.failed != true) {
                        failedIndices.add(FailedIndex(indexMetadata.index.name, indexMetadata.index.uuid, "This index is not in failed state."))
                    } else {
                        listOfIndexMetaData.add(Pair(indexMetadata.index,
                            managedIndexMetaData.copy(
                                step = null,
                                stepCompleted = false,
                                stepStartTime = null,
                                failed = false,
                                consumedRetries = 0,
                                transitionTo = startState ?: managedIndexMetaData.transitionTo,
                                info = mapOf("message" to "Attempting retry")
                            )))
                    }
                }
            }
        }
    }

    data class FailedIndex(val name: String, val uuid: String, val reason: String)

    companion object {
        const val RETRY_BASE_URI = "${IndexStateManagementPlugin.ISM_BASE_URI}/retry"
        const val FAILURES = "failures"
        const val FAILED_INDICES = "failed_indices"
        const val UPDATED_INDICES = "updated_indices"
    }
}
