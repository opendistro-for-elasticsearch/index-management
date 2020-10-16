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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.interceptor

import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class RollupInterceptorIT : RollupRestTestCase() {

    fun `test a basic aggregations`() {
        generateNYCTaxiData("source")

        val rollup = Rollup(
            id = "basic_sum_search",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
        }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 0,
                "aggs": {
                    "sum": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    },
                    "min": {
                        "min": {
                            "field": "passenger_count"
                        }
                    },
                    "max": {
                        "max": {
                            "field": "passenger_count"
                        }
                    },
                    "avg": {
                        "avg": {
                            "field": "passenger_count"
                        }
                    },
                    "value_count": {
                        "value_count": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals("Source and rollup index did not return same max results", rawAggRes["max"]!!["value"], rollupAggRes["max"]!!["value"])
        assertEquals("Source and rollup index did not return same min results", rawAggRes["min"]!!["value"], rollupAggRes["min"]!!["value"])
        assertEquals("Source and rollup index did not return same sum results", rawAggRes["sum"]!!["value"], rollupAggRes["sum"]!!["value"])
        assertEquals("Source and rollup index did not return same value_count results", rawAggRes["value_count"]!!["value"], rollupAggRes["value_count"]!!["value"])
        // TODO: This is not working - It seems we lose some precision when indexing the composite results into the rollup index because of double <-> float as we let dynamic mapping map to float
        // assertEquals("Source and rollup index did not return same avg results", rawAggRes["avg"]!!["value"], rollupAggRes["avg"]!!["value"])
    }

    fun `test a term query`() {
        generateNYCTaxiData("source")
        val rollup = Rollup(
                id = "basic_term_query",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source",
                targetIndex = "target",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions = listOf(
                        DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
                )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 0,
                "query": {
                    "term": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()

        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )
    }

    fun `test a terms query`() {
        generateNYCTaxiData("source")
        val rollup = Rollup(
                id = "basic_term_query",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source",
                targetIndex = "target",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions = listOf(
                        DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
                )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 0,
                "query": {
                    "terms": {
                        "RatecodeID": [1, 2]
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()

        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )
    }

    fun `test a range query`() {
        generateNYCTaxiData("source")
        val rollup = Rollup(
                id = "basic_term_query",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source",
                targetIndex = "target",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions = listOf(
                        DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
                )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 0,
                "query": {
                    "range": {
                        "RatecodeID": {"gte": 1, "lt":2}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()

        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )
    }

    fun `test a bool query`() {
        generateNYCTaxiData("source")
        val rollup = Rollup(
                id = "basic_term_query",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source",
                targetIndex = "target",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions = listOf(
                        DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID"),
                        Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
                )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 0,
                "query": {
                    "bool": {
                        "must_not": {"term": {"RatecodeID": 1}},
                        "must": {"range": {"RatecodeID": {"lte": 5}}},
                        "filter": {"term": {"PULocationID": 132}},
                        "should": {"range": {"PULocationID": {"gte": 100}}}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()

        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )
    }

    fun `test invalid size on rollup search`() {
        generateNYCTaxiData("source")

        val rollup = Rollup(
            id = "basic_sum_search",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
        }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
            {
                "size": 3,
                "aggs": { "sum": { "sum": { "field": "passenger_count" } } }
            }
        """.trimIndent()
        try {
            client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "Rollup search must have size explicitly set to 0, but found 3",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        val reqNoSize = """{ "aggs": { "sum": { "sum": { "field": "passenger_count" } } } }"""
        try {
            client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(reqNoSize, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "Rollup search must have size explicitly set to 0, but found -1",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
