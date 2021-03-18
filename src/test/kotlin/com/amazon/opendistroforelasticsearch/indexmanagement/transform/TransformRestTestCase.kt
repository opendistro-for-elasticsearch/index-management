package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.Response
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import java.time.Instant

abstract class TransformRestTestCase : IndexManagementRestTestCase() {

    override fun preserveIndicesUponCompletion(): Boolean = true

    protected fun createTransform(
        transform: Transform,
        transformId: String = ESTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true
    ): Transform {
        val response = createTransformJson(transform.toJsonString(), transformId, refresh)

        val transformJson = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.entity.content)
            .map()
        val createdId = transformJson["_id"] as String
        assertEquals("Transform ids are not the same", transformId, createdId)
        return transform.copy(
            id = createdId,
            seqNo = (transformJson["_seq_no"] as Int).toLong(),
            primaryTerm = (transformJson["_primary_term"] as Int).toLong()
        )
    }

    protected fun createTransformJson(
        transformString: String,
        transformId: String,
        refresh: Boolean = true
    ): Response {
        val response = client()
            .makeRequest(
                "PUT",
                "$TRANSFORM_BASE_URI/$transformId?refresh=$refresh",
                emptyMap(),
                StringEntity(transformString, APPLICATION_JSON)
            )
        assertEquals("Unable to create a new transform", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun createTransformSourceIndex(transform: Transform, settings: Settings = Settings.EMPTY) {
        var mappingString = ""
        var addCommaPrefix = false
        transform.groups.forEach {
            val fieldType = when (it.type) {
                Dimension.Type.DATE_HISTOGRAM -> "date"
                Dimension.Type.HISTOGRAM -> "long"
                Dimension.Type.TERMS -> "keyword"
            }
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"$fieldType\"}"
            addCommaPrefix = true
            mappingString += string
        }
        mappingString = "\"properties\":{$mappingString}"
        createIndex(transform.sourceIndex, settings, mappingString)
    }

    protected fun putDateDocumentInSourceIndex(transform: Transform) {
        val dateHistogram = transform.groups.first()
        val request = """
            {
              "${dateHistogram.sourceField}" : "${Instant.now()}"
            }
        """.trimIndent()
        val response = client().makeRequest(
            "POST",
            "${transform.sourceIndex}/_doc?refresh=true",
            emptyMap(),
            StringEntity(request, APPLICATION_JSON)
        )
        assertEquals("Request failed", RestStatus.CREATED, response.restStatus())
    }

    protected fun Transform.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)
}
