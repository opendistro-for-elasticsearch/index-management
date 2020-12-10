package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ISMTemplateService
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.ismTemplates
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.regex.Regex
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetISMTemplateAction::class.java)

// TransportGetComposableIndexTemplateAction
class TransportGetISMTemplateAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client,
    val ismTemplateService: ISMTemplateService
) : TransportMasterNodeAction<GetISMTemplateRequest, GetISMTemplateResponse>(
    GetISMTemplateAction.NAME,
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { GetISMTemplateRequest(it) },
    indexNameExpressionResolver
) {
    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    override fun read(sin: StreamInput): GetISMTemplateResponse {
        return GetISMTemplateResponse(sin)
    }

    override fun masterOperation(request: GetISMTemplateRequest, state: ClusterState, listener: ActionListener<GetISMTemplateResponse>) {
        val allTemplates = state.metadata.ismTemplates()
        if (request.templateNames.isEmpty()) {
            listener.onResponse(GetISMTemplateResponse(allTemplates))
            return
        }

        val results = mutableMapOf<String, ISMTemplate>()
        val reqTemplates = request.templateNames
        if (allTemplates.isEmpty()) throw ResourceNotFoundException("index template matching ${reqTemplates.toList()} not found")
        reqTemplates.forEach { name ->
            allTemplates.forEach { (templateName, template) ->
                when {
                    Regex.simpleMatch(name, templateName) -> results[templateName] = template
                    allTemplates.containsKey(name) -> results[templateName] = template
                    else -> throw ResourceNotFoundException("index template matching [$name] not found")
                }
            }
        }

        listener.onResponse(GetISMTemplateResponse(results))
    }

    override fun checkBlock(request: GetISMTemplateRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }
}
