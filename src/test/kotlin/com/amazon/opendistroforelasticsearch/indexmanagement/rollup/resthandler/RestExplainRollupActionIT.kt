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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
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
class RestExplainRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test explain rollup`() {
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

        // Add it in a waitFor because the metadata is not immediately available and it starts off in INIT for a very brief time
        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            assertNotNull("MetadataID on rollup was null", updatedRollup.metadataID)
            val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/${updatedRollup.id}/_explain")
            assertEquals(RestStatus.OK, response.restStatus())
            val responseMap = response.asMap()
            assertNotNull("Response is null", responseMap)
            assertTrue("Response does not have metadata", responseMap.keys.isNotEmpty())
            val explainMetadata = responseMap[updatedRollup.id] as Map<String, Any>
            assertNotNull("Did not have key for rollup ID", explainMetadata)
            assertEquals("Did not have metadata_id in explain response", updatedRollup.metadataID, explainMetadata["metadata_id"])
            val metadata = explainMetadata["rollup_metadata"] as Map<String, Any>
            assertNotNull("Did not have metadata in explain response", metadata)
            // It should be finished because we have no docs and it's not continuous so it should finish immediately upon execution
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED.type, metadata["status"])
        }
    }

    @Throws(Exception::class)
    fun `test explain a rollup with no id fails`() {
        try {
            val rollup = randomRollup()
            client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI//_explain", emptyMap(), rollup.toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test explain rollup for nonexistent id`() {
        // Creating a rollup so the config index exists
        createRollup(rollup = randomRollup(), rollupId = "some_other_id")
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/doesntexist/_explain")
        assertNull("Nonexistent rollup didn't return null", response.asMap()["doesntexist"])
    }

    @Throws(Exception::class)
    fun `test explain rollup for wildcard id`() {
        // Creating a rollup so the config index exists
        createRollup(rollup = randomRollup(), rollupId = "some_id")
        createRollup(rollup = randomRollup(), rollupId = "some_other_id")
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some*/_explain")
        // We don't expect there to always be metadata as we are creating random rollups and the job isn't running
        // but we do expect the wildcard some* to expand to the two jobs created above and have non-null values (meaning they exist)
        val map = response.asMap()
        assertNotNull("Non null some_id value wasn't in the response", map["some_id"])
        assertNotNull("Non null some_other_id value wasn't in the response", map["some_other_id"])
    }

    @Throws(Exception::class)
    fun `test explain rollup for job that hasnt started`() {
        createRollup(rollup = randomRollup().copy(metadataID = null), rollupId = "some_id")
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some_id/_explain")
        val expectedMap = mapOf("some_id" to mapOf("metadata_id" to null, "rollup_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain rollup for metadata_id but no metadata`() {
        // This is to test the case of a rollup existing with a metadataID but there being no metadata document
        createRollup(rollup = randomRollup().copy(metadataID = "some_metadata_id"), rollupId = "some_id")
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some_id/_explain")
        val expectedMap = mapOf("some_id" to mapOf("metadata_id" to "some_metadata_id", "rollup_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain rollup when config doesnt exist`() {
        val responseExplicit = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some_rollup/_explain")
        assertEquals("Non-existent rollup didn't return null", mapOf("some_rollup" to null), responseExplicit.asMap())

        val responseExplicitMultiple = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some_rollup,another_rollup/_explain")
        assertEquals("Multiple non-existent rollup didn't return null", mapOf("some_rollup" to null, "another_rollup" to null), responseExplicitMultiple.asMap())

        val responseWildcard = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/another_*/_explain")
        assertEquals("Wildcard rollup didn't return nothing", mapOf<String, Map<String, Any>?>(), responseWildcard.asMap())

        val responseMultipleTypes = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some_rollup,another_*/_explain")
        assertEquals("Non-existent and wildcard rollup didn't return only non-existent as null", mapOf("some_rollup" to null), responseMultipleTypes.asMap())
    }
}
