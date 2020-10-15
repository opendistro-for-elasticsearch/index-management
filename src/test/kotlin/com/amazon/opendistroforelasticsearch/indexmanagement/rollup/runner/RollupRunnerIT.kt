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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
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

    fun `test metadata set to failed when rollup job has a metadata id but metadata doc doesn't exist`() {
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

    fun `test metadata stats contains correct info`() {
        generateNYCTaxiData("source")

        val rollup = Rollup(
            id = "basic_stats_check",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic stats test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        val secondRollup = Rollup(
            id = "all_inclusive_intervals",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic stats test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "100d")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        val thirdRollup = Rollup(
            id = "second_interval",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic 1s test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)
        updateRollupStartTime(secondRollup)
        updateRollupStartTime(thirdRollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        val finishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        val secondFinishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        val thirdFinishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = thirdRollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        refreshAllIndices()

        val rollupMetadataID = finishedRollup.metadataID!!
        val rollupMetadata = getRollupMetadata(rollupMetadataID)
        val secondRollupMetadataID = secondFinishedRollup.metadataID!!
        val secondRollupMetadata = getRollupMetadata(secondRollupMetadataID)
        val thirdRollupMetadataID = thirdFinishedRollup.metadataID!!
        val thirdRollupMetadata = getRollupMetadata(thirdRollupMetadataID)

        // These might seem like magic numbers but they are static/fixed based off the dataset in the resources
        // We have two pages processed because afterKey is always returned if there is data in the response
        // So the first pagination returns an afterKey and the second doesn't
        assertEquals("Did not have 2 pages processed", 2L, rollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, rollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and an hourly rollup there
        // should be 10 buckets with data in them which means 10 rollup documents
        assertEquals("Did not have 10 rollups indexed", 10L, rollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", rollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", rollupMetadata.stats.searchTimeInMillis > 0L)

        assertEquals("Did not have 2 pages processed", 2L, secondRollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, secondRollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and a 100 day rollup there
        // should be 1 bucket with data in them which means 1 rollup documents
        assertEquals("Did not have 1 rollup indexed", 1L, secondRollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", secondRollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", secondRollupMetadata.stats.searchTimeInMillis > 0L)

        assertEquals("Did not have 28 pages processed", 28L, thirdRollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, thirdRollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and a 1 second rollup there
        // should be 2667 buckets with data in them which means 2667 rollup documents
        assertEquals("Did not have 2667 rollups indexed", 2667L, thirdRollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", thirdRollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", thirdRollupMetadata.stats.searchTimeInMillis > 0L)
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
