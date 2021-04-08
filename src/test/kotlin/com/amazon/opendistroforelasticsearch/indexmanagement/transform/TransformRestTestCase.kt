package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchModule
import org.elasticsearch.test.ESTestCase
import java.time.Duration
import java.time.Instant

abstract class TransformRestTestCase : IndexManagementRestTestCase() {

    override fun preserveIndicesUponCompletion(): Boolean = true

    protected fun createTransform(
        transform: Transform,
        transformId: String = ESTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true
    ): Transform {
        val response = createTransformJson(transform.toJsonString(), transformId, refresh)

        val transformJson = createParser(XContentType.JSON.xContent(), response.entity.content)
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

    protected fun createRandomTransform(refresh: Boolean = true): Transform {
        val transform = randomTransform()
        val transformId = createTransform(transform, refresh = refresh).id
        return getTransform(transformId = transformId)
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

    protected fun getTransform(
        transformId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    ): Transform {
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/$transformId", null, header)
        assertEquals("Unable to get transform $transformId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var transform: Transform

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                Transform.TRANSFORM_TYPE -> transform = Transform.parse(parser, id, seqNo, primaryTerm)
            }
        }
        return transform
    }

    protected fun updateTransformStartTime(update: Transform, desiredStartTimeMillis: Long? = null) {
        // Before updating start time of a job always make sure there are no unassigned shards that could cause the config
        // index to move to a new node and negate this forced start
        if (isMultiNode) {
            waitFor {
                try {
                    client().makeRequest("GET", "_cluster/allocation/explain")
                    fail("Expected 400 Bad Request when there are no unassigned shards to explain")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
                }
            }
        }
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: Instant.now().toEpochMilli() - millis
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val response = client().makeRequest("POST", "$INDEX_MANAGEMENT_INDEX/_update/${update.id}?wait_for_active_shards=$waitForActiveShards",
            StringEntity(
                "{\"doc\":{\"transform\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$startTimeMillis\"}}}}}",
                APPLICATION_JSON
            ))

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }

    protected fun Transform.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun newParser(response: Response): XContentParser {
        return XContentType.JSON.xContent().createParser(NamedXContentRegistry(SearchModule(Settings.EMPTY, false, emptyList()).namedXContents),
            LoggingDeprecationHandler.INSTANCE, response.entity.content)
    }
    override fun xContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(SearchModule(Settings.EMPTY, false, emptyList()).namedXContents)
    }
}
