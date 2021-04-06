package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.ExplainTransform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
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
import org.elasticsearch.index.query.WildcardQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.transport.TransportService

class TransportExplainTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExplainTransformRequest, ExplainTransformResponse>(
    ExplainTransformAction.NAME, transportService, actionFilters, ::ExplainTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainTransformRequest, actionListener: ActionListener<ExplainTransformResponse>) {
        val ids = request.transformIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainTransform?> = ids.filter { !it.contains("*") }
            .map { it to null }.toMap(mutableMapOf())
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
            .source(SearchSourceBuilder().query(
                BoolQueryBuilder().minimumShouldMatch(1).apply {
                    ids.forEach {
                        this.should(WildcardQueryBuilder("${ Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$it*"))
                    }
                }
            ))
        client.search(searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                try {
                    response.hits.hits.forEach {
                        val transform = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Transform.Companion::parse)
                        idsToExplain[transform.id] = ExplainTransform(metadataID = transform.metadataId)
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
                                val metadata = contentParser(it.sourceRef)
                                    .parseWithType(it.id, it.seqNo, it.primaryTerm, TransformMetadata.Companion::parse)
                                idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                    explainTransform.copy(metadata = metadata) }
                            }
                            actionListener.onResponse(ExplainTransformResponse(idsToExplain.toMap()))
                        } catch (e: Exception) {
                            log.error("Failed to parse transform metadata", e)
                            actionListener.onFailure(e)
                            return
                        }
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search transform metadata", e)
                        when (e) {
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                })
            }

            override fun onFailure(e: Exception) {
                log.error("Failed to search for transforms", e)
                when (e) {
                    is ResourceNotFoundException -> {
                        val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                        actionListener.onResponse(ExplainTransformResponse(nonWildcardIds))
                    }
                    is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                    else -> actionListener.onFailure(e)
                }
            }
        })
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
    }
}
