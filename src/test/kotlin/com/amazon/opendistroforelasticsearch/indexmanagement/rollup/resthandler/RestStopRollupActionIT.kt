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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestStopRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test stopping a started rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = true, jobEnabledTime = randomInstant(), metadataID = null))
        assertTrue("Rollup was not enabled", rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a stopped rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = true, jobEnabledTime = randomInstant(), metadataID = null))
        assertTrue("Rollup was not enabled", rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)

        val secondResponse = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        val updatedSecondRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedSecondRollup.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a finished rollup`() {
        // Create a rollup that finishes
        val rollup = createRollup(
            randomRollup()
                .copy(
                    continuous = false,
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    jobEnabledTime = Instant.now(),
                    metadataID = null
                )
        )
        createRollupSourceIndex(rollup)
        updateRollupStartTime(rollup)

        // Assert it finished
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never finished", RollupMetadata.Status.FINISHED, metadata.status)
            // Waiting for job to be disabled here to avoid version conflict exceptions later on
            assertFalse("Job was not disabled", updatedRollup.enabled)
        }

        // Try to stop a finished rollup
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert it is still in finished status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed finished", RollupMetadata.Status.FINISHED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a failed rollup`() {
        // Create a rollup that will fail because no source index
        val rollup = randomRollup().copy(
            id = "test_stopping_a_failed_rollup",
            continuous = false,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null
        ).let { createRollup(it, it.id) }
        updateRollupStartTime(rollup)

        // Assert its in failed
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never failed", RollupMetadata.Status.FAILED, metadata.status)
        }

        // Stop rollup
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert rollup still failed status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed failed", RollupMetadata.Status.FAILED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a retry rollup`() {
        // Create a rollup job
        val rollup = createRollup(
            randomRollup()
                .copy(
                    continuous = false,
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    jobEnabledTime = Instant.now(),
                    metadataID = null
                )
        )

        // Force rollup to execute which should fail as we did not create a source index
        updateRollupStartTime(rollup)

        // Assert rollup is in failed status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never failed (no source index)", RollupMetadata.Status.FAILED, metadata.status)
        }

        // Start job to set it into retry status
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert the job is in retry status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup is not in RETRY", RollupMetadata.Status.RETRY, metadata.status)
        }

        // Stop the job which is currently in retry status
        val responseTwo = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, responseTwo.restStatus())
        val expectedResponseTwo = mapOf("acknowledged" to true)
        assertEquals(expectedResponseTwo, responseTwo.asMap())

        // Assert the job correctly went back to failed and not stopped
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed finished", RollupMetadata.Status.FAILED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping rollup with metadata`() {
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
            metrics = emptyList()
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not STARTED", RollupMetadata.Status.STARTED, rollupMetadata.status)

            // There are two calls to _stop happening serially which is prone to version conflicts during an ongoing job
            // so including it in a waitFor to ensure it can retry a few times
            val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
            assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
            val expectedResponse = mapOf("acknowledged" to true)
            assertEquals(expectedResponse, response.asMap())
        }

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)
        val rollupMetadata = getRollupMetadata(updatedRollup.metadataID!!)
        assertEquals("Rollup is not STOPPED", RollupMetadata.Status.STOPPED, rollupMetadata.status)
    }

    @Throws(Exception::class)
    fun `test stop a rollup with no id fails`() {
        try {
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI//_stop")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
