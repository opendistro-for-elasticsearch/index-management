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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
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
class RestStartRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test starting a stopped rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = false, jobEnabledTime = null, metadataID = null))
        assertTrue("Rollup was not disabled", !rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)
    }

    // TODO: With and without metadata
    @Throws(Exception::class)
    fun `test starting a started rollup doesnt change enabled time`() {
        // First create a non-started rollup
        val rollup = createRollup(randomRollup().copy(enabled = false, jobEnabledTime = null, metadataID = null))
        assertTrue("Rollup was not disabled", !rollup.enabled)

        // Enable it to get the job enabled time
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)

        val secondResponse = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        // Confirm the job enabled time is not reset to a newer time if job was already enabled
        val updatedSecondRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedSecondRollup.enabled)
        assertEquals("Jobs had different enabled times", updatedRollup.jobEnabledTime, updatedSecondRollup.jobEnabledTime)
    }

    @Throws(Exception::class)
    fun `test start a rollup with no id fails`() {
        try {
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI//_start")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test starting a failed rollup`() {
        val rollup = Rollup(
            id = "restart_failed_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_failed_rollup",
            targetIndex = "target_restart_failed_rollup",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = emptyList()
        ).let { createRollup(it, it.id) }

        // This should fail because we did not create a source index
        updateRollupStartTime(rollup)

        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            assertNotNull("MetadataID on rollup was null", updatedRollup.metadataID)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // It should be failed because we did not create the source index
            assertEquals("Status should be failed", RollupMetadata.Status.FAILED, metadata.status)
            assertFalse("Rollup was not disabled", updatedRollup.enabled)
        }

        // Now create the missing source index
        generateNYCTaxiData("source_restart_failed_rollup")

        // And call _start on the failed rollup job
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)
        waitFor {
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // It should be in retry now
            assertEquals("Status should be retry", RollupMetadata.Status.RETRY, metadata.status)
        }

        updateRollupStartTime(rollup)

        // Rollup should be able to finished, with actual rolled up docs
        waitFor {
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not roll up documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not roll up documents", metadata.stats.rollupsIndexed > 0)
        }
    }

    @Throws(Exception::class)
    fun `test starting a finished rollup`() {
        generateNYCTaxiData("source_restart_finished_rollup")
        val rollup = Rollup(
            id = "restart_finished_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_finished_rollup",
            targetIndex = "target_restart_finished_rollup",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = emptyList()
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)
        var firstRollupsIndexed = 0L
        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not roll up documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not roll up documents", metadata.stats.rollupsIndexed > 0)
            firstRollupsIndexed = metadata.stats.rollupsIndexed
        }

        deleteIndex("target_restart_finished_rollup")

        // And call _start on the failed rollup job
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        updateRollupStartTime(rollup)

        // Rollup should be able to finished, with actual rolled up docs again
        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // logger.info("metadata $metadata")
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            // Expect 10k docs now (5k from first and 5k from second)
            assertEquals("Did not roll up documents", 10000, metadata.stats.documentsProcessed)
            // Should have twice the rollups indexed now
            assertEquals("Did not index rollup docs", firstRollupsIndexed * 2, metadata.stats.rollupsIndexed)
            assertIndexExists("target_restart_finished_rollup")
        }
    }
}
