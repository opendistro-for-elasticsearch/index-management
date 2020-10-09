/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.Response
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import java.time.Duration
import java.time.Instant

abstract class RollupRestTestCase : IndexManagementRestTestCase() {

    protected fun createRollup(
        rollup: Rollup,
        rollupId: String = ESTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true
    ): Rollup {
        val response = createRollupJson(rollup.toJsonString(), rollupId, refresh)

        val rollupJson = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.entity.content)
            .map()
        val createdId = rollupJson["_id"] as String
        assertEquals("Rollup ids are not the same", rollupId, createdId)
        return rollup.copy(
            id = createdId,
            seqNo = (rollupJson["_seq_no"] as Int).toLong(),
            primaryTerm = (rollupJson["_primary_term"] as Int).toLong()
        )
    }

    protected fun createRollupJson(
        rollupString: String,
        rollupId: String,
        refresh: Boolean = true
    ): Response {
        val response = client()
            .makeRequest(
                "PUT",
                "$ROLLUP_JOBS_BASE_URI/$rollupId?refresh=$refresh",
                emptyMap(),
                StringEntity(rollupString, APPLICATION_JSON)
            )
        assertEquals("Unable to create a new rollup", RestStatus.CREATED, response.restStatus())
        return response
    }

    protected fun createRandomRollup(refresh: Boolean = true): Rollup {
        val rollup = randomRollup()
        val rollupId = createRollup(rollup, refresh = refresh).id
        return getRollup(rollupId = rollupId)
    }

    // TODO: Maybe clean-up and use XContentFactory.jsonBuilder() to create mappings json
    protected fun createRollupSourceIndex(rollup: Rollup, settings: Settings = Settings.EMPTY) {
        var mappingString = ""
        var addCommaPrefix = false
        rollup.dimensions.forEach {
            val fieldType = when (it.type) {
                Dimension.Type.DATE_HISTOGRAM -> "date"
                Dimension.Type.HISTOGRAM -> "long"
                Dimension.Type.TERMS -> "keyword"
            }
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"$fieldType\"}"
            addCommaPrefix = true
            mappingString += string
        }
        rollup.metrics.forEach {
            val string = "${if (addCommaPrefix) "," else ""}\"${it.sourceField}\":{\"type\": \"long\"}"
            addCommaPrefix = true
            mappingString += string
        }
        mappingString = "\"properties\":{$mappingString}"
        createIndex(rollup.sourceIndex, settings, mappingString)
    }

    protected fun putDateDocumentInSourceIndex(rollup: Rollup) {
        val dateHistogram = rollup.dimensions.first()
        val request = """
            {
              "${dateHistogram.sourceField}" : "${Instant.now()}"
            }
        """.trimIndent()
        val response = client().makeRequest("POST", "${rollup.sourceIndex}/_doc?refresh=true",
            emptyMap(), StringEntity(request, APPLICATION_JSON))
        assertEquals("Request failed", RestStatus.CREATED, response.restStatus())
    }

    protected fun getRollup(
        rollupId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    ): Rollup {
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/$rollupId", null, header)
        assertEquals("Unable to get rollup $rollupId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(
            XContentParser.Token.START_OBJECT,
            parser.nextToken(),
            parser::getTokenLocation
        )

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var rollup: Rollup

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                Rollup.ROLLUP_TYPE -> rollup = Rollup.parse(parser)
            }
        }
        return rollup.copy(id = id, seqNo = seqNo, primaryTerm = primaryTerm)
    }

    protected fun getRollupMetadata(
        metadataId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    ): RollupMetadata {
        val response = client().makeRequest("GET", "$INDEX_MANAGEMENT_INDEX/_doc/$metadataId", null, header)
        assertEquals("Unable to get rollup metadata $metadataId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(
            XContentParser.Token.START_OBJECT,
            parser.nextToken(),
            parser::getTokenLocation
        )

        lateinit var id: String
        var primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        var seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO
        lateinit var metadata: RollupMetadata

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                _ID -> id = parser.text()
                _SEQ_NO -> seqNo = parser.longValue()
                _PRIMARY_TERM -> primaryTerm = parser.longValue()
                RollupMetadata.ROLLUP_METADATA_TYPE -> metadata = RollupMetadata.parse(parser)
            }
        }

        return metadata.copy(id = id, seqNo = seqNo, primaryTerm = primaryTerm)
    }

    protected fun Rollup.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), APPLICATION_JSON)

    protected fun updateRollupStartTime(update: Rollup, desiredStartTimeMillis: Long? = null) {
        val intervalSchedule = (update.jobSchedule as IntervalSchedule)
        val millis = Duration.of(intervalSchedule.interval.toLong(), intervalSchedule.unit).minusSeconds(2).toMillis()
        val startTimeMillis = desiredStartTimeMillis ?: Instant.now().toEpochMilli() - millis
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val response = client().makeRequest("POST", "$INDEX_MANAGEMENT_INDEX/_update/${update.id}?wait_for_active_shards=$waitForActiveShards",
            StringEntity(
                "{\"doc\":{\"rollup\":{\"schedule\":{\"interval\":{\"start_time\":" +
                    "\"$startTimeMillis\"}}}}}",
                APPLICATION_JSON
            ))

        assertEquals("Request failed", RestStatus.OK, response.restStatus())
    }
}
