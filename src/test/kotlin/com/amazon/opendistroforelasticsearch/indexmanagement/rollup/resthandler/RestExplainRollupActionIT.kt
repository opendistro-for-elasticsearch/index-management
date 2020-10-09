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
            val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_explain")
            assertEquals(RestStatus.OK, response.restStatus())
            val responseMap = response.asMap()
            assertNotNull("Response is null", responseMap)
            assertTrue("Response does not have metadata", responseMap.keys.isNotEmpty())
            val metadata = responseMap[rollup.id] as Map<String, Any>
            assertNotNull("Did not have key for rollup ID", metadata)
            assertEquals("Rollup id is not correct", rollup.id, metadata["rollup_id"])
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

    // TODO: Check rollup_id with a dash and searching wildcard -> handle tokenizer/analyzer

    // TODO
    @Throws(Exception::class)
    fun `skip test explain rollup for wildcard id`() {
        // Creating a rollup so the config index exists
        createRollup(rollup = randomRollup(), rollupId = "some_id")
        createRollup(rollup = randomRollup(), rollupId = "some_other_id")
        val response = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI/some*/_explain")
        logger.info("response ${response.asMap()}")
        fail()
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
