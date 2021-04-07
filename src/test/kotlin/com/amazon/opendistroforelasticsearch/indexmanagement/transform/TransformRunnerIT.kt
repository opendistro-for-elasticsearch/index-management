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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.elasticsearch.index.query.TermQueryBuilder

class TransformRunnerIT : TransformRestTestCase() {

    fun `test transforms`() {
        generateNYCTaxiData("transform-source-index")

        var transform = Transform(
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

        assertTrue("Source transform index was not created", indexExists(transform.sourceIndex))
        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        var metadata = waitFor {
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
        transform = Transform(
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

        metadata = waitFor {
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

        // With invalid mapping
        transform = Transform(
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

        metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }
}
