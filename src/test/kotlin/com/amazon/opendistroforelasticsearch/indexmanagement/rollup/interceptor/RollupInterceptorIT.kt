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

    fun `test roll up search`() {
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
                        DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID"),
                        Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(),
                                ValueCount(), Average())),
                        RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
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

        // Term query
        var req = """
            {
                "size": 0,
                "query": {
                    "term": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        var rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        var rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Terms query
        req = """
            {
                "size": 0,
                "query": {
                    "terms": {
                        "RatecodeID": [1, 2]
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Range query
        req = """
            {
                "size": 0,
                "query": {
                    "range": {
                        "RatecodeID": {"gte": 1, "lt":2}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Bool query
        req = """
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
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Boost query
        req = """
            {
                "size": 0,
                "query": {
                    "boosting": {
                       "positive": {"range": {"RatecodeID": {"gte": 1}}},
                       "negative": {"term": {"RatecodeID": 2}},
                       "negative_boost": 1.0
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Const score query
        req = """
            {
                "size": 0,
                "query": {
                    "constant_score": {
                       "filter": {"range": {"RatecodeID": {"gt": 1}}},
                       "boost": 1.2
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Dis max query
        req = """
            {
                "size": 0,
                "query": {
                    "dis_max": {
                        "queries": [
                           {"range": {"RatecodeID": {"gt": 1}}},
                           {"bool": {"filter": {"term": {"RatecodeID": 2}}}}
                        ],
                        "tie_breaker": 0.8
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
                "Source and rollup index did not return same min results",
                rawAggRes.getValue("min_passenger_count")["value"],
                rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Unsupported query
        req = """
            {
                "size": 0,
                "query": {
                    "match": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        try {
            client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                    "Wrong error message",
                    "The match query is currently not supported in rollups",
                    (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No valid job for rollup search
        req = """
            {
                "size": 0,
                "query": {
                    "bool": {
                        "must": {"range": {"RateCodeID": {"gt": 1}}},
                        "filter": {"term": {"timestamp": "2020-10-03T00:00:00.000Z"}}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "total_amount"
                        }
                    }
                }
            }
        """.trimIndent()
        try {
            client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                    "Wrong error message",
                    "Could not find a rollup job that can answer this query because [missing field RateCodeID, missing field timestamp, " +
                            "missing sum aggregation on total_amount]",
                    (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No query just aggregations
        req = """
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
        rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals("Source and rollup index did not return same max results", rawAggRes["max"]!!["value"], rollupAggRes["max"]!!["value"])
        assertEquals("Source and rollup index did not return same min results", rawAggRes["min"]!!["value"], rollupAggRes["min"]!!["value"])
        assertEquals("Source and rollup index did not return same sum results", rawAggRes["sum"]!!["value"], rollupAggRes["sum"]!!["value"])
        assertEquals("Source and rollup index did not return same value_count results", rawAggRes["value_count"]!!["value"], rollupAggRes["value_count"]!!["value"])
        assertEquals("Source and rollup index did not return same avg results", rawAggRes["avg"]!!["value"], rollupAggRes["avg"]!!["value"])

        // Invalid size in search - size > 0
        req = """
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

        // Invalid size in search - missing size
        req = """{ "aggs": { "sum": { "sum": { "field": "passenger_count" } } } }"""
        try {
            client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
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

    fun `test bucket and sub aggregations have correct values`() {
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
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(),
                    ValueCount(), Average())),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
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

        // No query just bucket and sub metric aggregations
        val req = """
            {
                "size": 0,
                "aggs": {
                    "pickup_areas": {
                        "terms": { "field": "PULocationID", "size": 1000, "order": { "_key": "asc" } },
                        "aggs": {
                          "sum": { "sum": { "field": "passenger_count" } },
                          "min": { "min": { "field": "passenger_count" } },
                          "max": { "max": { "field": "passenger_count" } },
                          "avg": { "avg": { "field": "passenger_count" } },
                          "value_count": { "value_count": { "field": "passenger_count" } }
                        }
                    }
                }
            }
        """.trimIndent()
        val rawRes = client().makeRequest("POST", "/source/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)["pickup_areas"]!!["buckets"]!!
        val rollupAggBuckets = (rollupRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)["pickup_areas"]!!["buckets"]!!

        assertEquals("Different bucket sizes", rawAggBuckets.size, rollupAggBuckets.size)
        rawAggBuckets.forEachIndexed { idx, rawAggBucket ->
            val rollupAggBucket = rollupAggBuckets[idx]
            assertEquals("The sum aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["sum"]!!["value"], rollupAggBucket["sum"]!!["value"])
            assertEquals("The max aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["max"]!!["value"], rollupAggBucket["max"]!!["value"])
            assertEquals("The min aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["min"]!!["value"], rollupAggBucket["min"]!!["value"])
            assertEquals("The value_count aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["value_count"]!!["value"], rollupAggBucket["value_count"]!!["value"])
            assertEquals("The avg aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["avg"]!!["value"], rollupAggBucket["avg"]!!["value"])
        }
    }
}
