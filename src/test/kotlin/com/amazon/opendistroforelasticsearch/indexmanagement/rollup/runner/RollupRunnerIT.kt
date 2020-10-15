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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.runner

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomCalendarDateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit

class RollupRunnerIT : RollupRestTestCase() {

    fun `test metadata is created for rollup job when none exists`() {
        val indexName = "test_index"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = false
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            // Non-continuous job will finish in a single execution
            assertEquals("Unexpected metadata state", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }

    // TODO: Need to add next_window_end_time logic and fix version conflict exception before running this
    fun `skip test metadata set to failed when rollup job has a metadata id but metadata doc doesn't exist`() {
        val indexName = "test_index"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var previousRollupMetadata: RollupMetadata? = null
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            previousRollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", previousRollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.INIT, previousRollupMetadata!!.status)
        }

        // Delete rollup metadata
        assertNotNull("Previous rollup metadata was not saved", previousRollupMetadata)
        deleteRollupMetadata(previousRollupMetadata!!.id)

        // Update rollup start time to run second execution
        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            assertNotEquals("Rollup job metadata was not changed", previousRollupMetadata!!.id, rollupJob.metadataID)

            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            assertEquals("Unexpected metadata state", RollupMetadata.Status.FAILED, rollupMetadata.status)
        }

        // TODO: Call _start to retry and test recovery behavior
    }

    // NOTE: The test document being added for creating the start/end windows has the timestamp of Instant.now().
    // It's possible that this timestamp can fall on the very edge of the endtime and therefore execute the second time around
    // which could result in this test failing.
    // Setting the interval to something large to minimize this scenario.
    fun `test no-op execution when a full window of time to rollup is not available`() {
        val indexName = "test_index"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true,
            dimensions = listOf(
                randomCalendarDateHistogram().copy(
                    calendarInterval = "1y"
                )
            )
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var previousRollupMetadata: RollupMetadata? = null
        // Assert on first execution
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            previousRollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", previousRollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.INIT, previousRollupMetadata!!.status)
        }

        assertNotNull("Previous rollup metadata was not saved", previousRollupMetadata)

        // Update rollup start time to run second execution
        updateRollupStartTime(rollup)

        // Wait some arbitrary amount of time so the execution happens
        // Not using waitFor since this is testing a lack of state change
        Thread.sleep(10000)

        // Assert that no changes were made
        val currentMetadata = getRollupMetadata(previousRollupMetadata!!.id)
        assertEquals("Rollup metadata was updated", previousRollupMetadata!!.lastUpdatedTime, currentMetadata.lastUpdatedTime)
    }

    fun `test running job with no source index fails`() {
        val indexName = "test_index"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true
        )

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var rollupMetadata: RollupMetadata?
        // Assert on first execution
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.FAILED, rollupMetadata!!.status)
            assertEquals("Unexpected failure reason", "Invalid source index", rollupMetadata!!.failureReason)
        }

        // TODO: Call _start to retry and test recovery behavior?
    }

    // TODO: Test scenarios:
    // - Source index deleted after first execution
    //      * If this is with a source index pattern and the underlying indices are recreated but with different data
    //        what would the behavior be? Restarting the rollup would cause there to be different data for the previous windows
    // - Invalid source index mappings
    // - Target index deleted after first execution
    // - Source index with pattern
    // - Source index with pattern with invalid indices
    // - Source index with pattern mapping to some closed indices

    private fun deleteRollupMetadata(metadataId: String) {
        val response = client().makeRequest("DELETE", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_doc/$metadataId")
        assertEquals("Unable to delete rollup metadata $metadataId", RestStatus.OK, response.restStatus())
    }
}
