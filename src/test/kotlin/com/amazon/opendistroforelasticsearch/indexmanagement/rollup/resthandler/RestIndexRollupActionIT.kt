/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Metric
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollupDimensions
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestIndexRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test creating a rollup`() {
        val rollup = randomRollup()
        val response = client().makeRequest("PUT", "$ROLLUP_JOBS_BASE_URI/${rollup.id}", emptyMap(), rollup.toHttpEntity())
        assertEquals("Create rollup failed", RestStatus.CREATED, response.restStatus())
        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        val createdSeqNo = responseBody[_SEQ_NO] as Int
        val createdPrimaryTerm = responseBody[_PRIMARY_TERM] as Int
        assertNotEquals("Response is missing Id", Rollup.NO_ID, createdId)
        assertEquals("Not same id", rollup.id, createdId)
        assertEquals("Incorrect seqNo", 0, createdSeqNo)
        assertEquals("Incorrect primaryTerm", 1, createdPrimaryTerm)
        assertEquals("Incorrect Location header", "$ROLLUP_JOBS_BASE_URI/$createdId", response.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test creating a rollup with no id fails`() {
        try {
            val rollup = randomRollup()
            client().makeRequest("PUT", ROLLUP_JOBS_BASE_URI, emptyMap(), rollup.toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a rollup with POST fails`() {
        try {
            val rollup = randomRollup()
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/some_rollup", emptyMap(), rollup.toHttpEntity())
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test mappings after rollup creation`() {
        createRandomRollup()

        val response = client().makeRequest("GET", "/$INDEX_MANAGEMENT_INDEX/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[INDEX_MANAGEMENT_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(XContentType.JSON.xContent(), javaClass.classLoader.getResource("mappings/opendistro-ism-config.json")
            .readText())
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    @Throws(Exception::class)
    fun `test update rollup with wrong seq_no and primary_term`() {
        val rollup = createRandomRollup()

        try {
            client().makeRequest("PUT",
                    "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=10251989&if_primary_term=2342",
                    emptyMap(), rollup.toHttpEntity())
            fail("expected 409 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test update rollup with correct seq_no and primary_term`() {
        val rollup = createRandomRollup()
        val updateResponse = client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.toHttpEntity())

        assertEquals("Update rollup failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        val updatedId = responseBody[_ID] as String
        val updatedSeqNo = (responseBody[_SEQ_NO] as Int).toLong()
        assertNotEquals("response is missing Id", Rollup.NO_ID, updatedId)
        assertEquals("not same id", rollup.id, updatedId)
        assertEquals("incorrect seqNo", rollup.seqNo + 1, updatedSeqNo)
    }

    @Throws(Exception::class)
    fun `test updating rollup source index`() {
        try {
            val rollup = createRandomRollup()
            client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.copy(sourceIndex = "something_different").toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [source_index]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [source_index]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    @Throws(Exception::class)
    fun `test updating rollup target index`() {
        try {
            val rollup = createRandomRollup()
            client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.copy(targetIndex = "something_different").toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [target_index]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [target_index]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    @Throws(Exception::class)
    fun `test updating rollup continuous field`() {
        try {
            val rollup = createRandomRollup()
            client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.copy(continuous = !rollup.continuous).toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [continuous]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [continuous]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    @Throws(Exception::class)
    fun `test updating rollup dimensions`() {
        try {
            val dimensions = randomRollupDimensions()
            val newDimensions: List<Dimension> = dimensions.map {
                when (it.type) {
                    Dimension.Type.DATE_HISTOGRAM -> (it as DateHistogram).copy(timezone = ESTestCase.randomZone())
                    Dimension.Type.HISTOGRAM -> (it as Histogram).copy(interval = 5.5)
                    Dimension.Type.TERMS -> (it as Terms).copy(targetField = "some_other_target_field")
                }
            }
            val rollup = createRollup(rollup = randomRollup().copy(dimensions = dimensions))
            client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.copy(dimensions = newDimensions).toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [dimensions]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [dimensions]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    @Throws(Exception::class)
    fun `test updating rollup metrics`() {
        try {
            val metrics = listOf(randomRollupMetrics(), randomRollupMetrics())
            val newMetrics: List<RollupMetrics> = metrics.map {
                it.copy(
                    metrics = it.metrics.map {
                        when (it.type) {
                            Metric.Type.AVERAGE -> Max()
                            Metric.Type.MAX -> Min()
                            Metric.Type.MIN -> Sum()
                            Metric.Type.SUM -> ValueCount()
                            Metric.Type.VALUE_COUNT -> Average()
                        }
                    }
                )
            }
            val rollup = createRollup(rollup = randomRollup().copy(metrics = metrics))
            client().makeRequest("PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true&if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(), rollup.copy(metrics = newMetrics).toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "status_exception", "reason" to "Not allowed to modify [metrics]")
                    ),
                    "type" to "status_exception",
                    "reason" to "Not allowed to modify [metrics]"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }
}
