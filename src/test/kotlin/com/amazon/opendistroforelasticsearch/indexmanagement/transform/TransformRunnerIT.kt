/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder

class TransformRunnerIT : TransformRestTestCase() {

    fun `test transform`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        val transform = Transform(
            id = "id_1",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 2L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 2L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 5000L, metadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)

        // With data filter
    }
    fun `test transform with data filter`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        val transform = Transform(
            id = "id_2",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            dataSelectionQuery = TermQueryBuilder("store_and_fwd_flag", "N")
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 2L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 1L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 4977L, metadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
    }

    fun `test invalid transform`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        // With invalid mapping
        val transform = Transform(
            id = "id_3",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "non-existent", targetField = "flag")
            )
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    fun `test transform with aggregations`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.max("min_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.min("max_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.avg("avg_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.count("count").field("orderID"))
        aggregatorFactories.addAggregator(AggregationBuilders.percentiles("passenger_distribution").percentiles(90.0, 95.0).field("passenger_count"))
        aggregatorFactories.addAggregator(
            ScriptedMetricAggregationBuilder("average_revenue_per_passenger_per_trip")
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.count = 0; state.sum = 0;", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "state.sum += doc[\"total_amount\"].value; state.count += doc[\"passenger_count\"].value",
                        emptyMap()
                    )
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                        emptyMap()
                    )
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                        emptyMap()
                    )
                )
        )

        val transform = Transform(
            id = "id_4",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            aggregations = aggregatorFactories
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 3L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 2L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 5000L, metadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
    }

    fun `test transform with failure during indexing`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        // Indexing failure because target index is strictly mapped
        createIndex("transform-target-strict-index", Settings.EMPTY, getStrictMappings())
        waitFor {
            assertTrue("Strict target index not created", indexExists("transform-target-strict-index"))
        }
        val transform = Transform(
            id = "id_5",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-strict-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    fun `test transform with invalid aggregation triggering search failure`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(
            ScriptedMetricAggregationBuilder("average_revenue_per_passenger_per_trip")
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.count = 0; state.sum = 0;", emptyMap()))
                .mapScript(Script(
                    ScriptType.INLINE,
                    Script.DEFAULT_SCRIPT_LANG,
                    "state.sum += doc[\"random_field\"].value; state.count += doc[\"passenger_count\"].value",
                    emptyMap()))
                .combineScript(Script(
                    ScriptType.INLINE,
                    Script.DEFAULT_SCRIPT_LANG,
                    "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                    emptyMap()))
                .reduceScript(Script(
                    ScriptType.INLINE,
                    Script.DEFAULT_SCRIPT_LANG,
                    "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                    emptyMap()
                ))
        )

        val transform = Transform(
            id = "id_6",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
                Histogram(sourceField = "passenger_count", targetField = "count", interval = 2.0),
                DateHistogram(sourceField = "tpep_pickup_datetime", targetField = "date", fixedInterval = "1d")
            ),
            aggregations = aggregatorFactories
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    private fun getStrictMappings(): String {
        return """
            "dynamic": "strict",
            "properties": {
                "some-column": {
                    "type": "keyword"
                }
            }
        """.trimIndent()
    }
}
