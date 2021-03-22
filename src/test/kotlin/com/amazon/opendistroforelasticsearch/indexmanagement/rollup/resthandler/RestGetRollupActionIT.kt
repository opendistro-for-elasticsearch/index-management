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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SIZE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging
import org.elasticsearch.test.rest.ESRestTestCase.randomList

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestGetRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test getting a rollup`() {
        var rollup = createRollup(randomRollup())
        val indexedRollup = getRollup(rollup.id)
        // Schema version and last updated time are updated during the creation so we need to update the original too for comparison
        // Job schedule interval will have a dynamic start time
        rollup = rollup.copy(
            schemaVersion = indexedRollup.schemaVersion,
            jobLastUpdatedTime = indexedRollup.jobLastUpdatedTime,
            jobSchedule = indexedRollup.jobSchedule
        )
        assertEquals("Indexed and retrieved rollup differ", rollup, indexedRollup)
    }

    @Throws(Exception::class)
    fun `test getting a rollup that doesn't exist`() {
        try {
            getRollup(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting all rollups`() {
        val rollups = randomList(1, 15) { createRollup(randomRollup()) }

        // Using a larger response size than the default in case leftover rollups prevent the ones created in this test from being returned
        val res = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI?size=100")
        val map = res.asMap()
        val totalRollups = map["total_rollups"] as Int
        val resRollups = map["rollups"] as List<Map<String, Any?>>

        // There can be leftover rollups from previous tests, so we will have at least rollups.size or more
        assertTrue("Total rollups was not the same", rollups.size <= totalRollups)
        assertTrue("Rollups response has different size", rollups.size <= resRollups.size)
        for (testRollup in rollups) {
            val foundRollup = resRollups.find { testRollup.id == it["_id"] as String }
            assertNotNull("Did not find matching rollup that should exist", foundRollup)
            val innerRollup = foundRollup!!["rollup"] as Map<String, Any?>
            assertEquals(testRollup.id, foundRollup["_id"] as String)
            assertEquals(testRollup.seqNo, (foundRollup["_seq_no"] as Int).toLong())
            assertEquals(testRollup.primaryTerm, (foundRollup["_primary_term"] as Int).toLong())
            assertEquals(testRollup.id, innerRollup["rollup_id"] as String)
            // Doesn't matter what rollup sets, current system is at schema version 7
            assertEquals(7, (innerRollup["schema_version"] as Int).toLong())
            assertEquals(testRollup.enabled, innerRollup["enabled"] as Boolean)
            assertEquals(testRollup.enabledTime?.toEpochMilli(), (innerRollup["enabled_time"] as Number?)?.toLong())
            // Last updated time will never be correct as it gets updated in the API call
            // assertEquals(testRollup.lastUpdateTime.toEpochMilli(), innerRollup["last_updated_time"] as Long)
            assertEquals(testRollup.continuous, innerRollup["continuous"] as Boolean)
            assertEquals(testRollup.targetIndex, innerRollup["target_index"] as String)
            assertEquals(testRollup.sourceIndex, innerRollup["source_index"] as String)
            assertEquals(testRollup.metadataID, innerRollup["metadata_id"] as String?)
            assertEquals(testRollup.roles, innerRollup["roles"] as List<String>)
            assertEquals(testRollup.pageSize, innerRollup["page_size"] as Int)
            assertEquals(testRollup.description, innerRollup["description"] as String)
            assertEquals(testRollup.delay, (innerRollup["delay"] as Number?)?.toLong())
            assertEquals(testRollup.metrics.size, (innerRollup["metrics"] as List<Map<String, Any?>>).size)
            assertEquals(testRollup.dimensions.size, (innerRollup["dimensions"] as List<Map<String, Any?>>).size)
        }
    }

    @Throws(Exception::class)
    fun `test changing response size when getting rollups`() {
        // Ensure at least more rollup jobs than the default (20) exists
        val rollupCount = 25
        repeat(rollupCount) { createRollup(randomRollup()) }

        // The default response size is 20, so even though 25 rollup jobs were made, at most 20 will be returned
        var res = client().makeRequest("GET", ROLLUP_JOBS_BASE_URI)
        var map = res.asMap()
        var resRollups = map["rollups"] as List<Map<String, Any?>>

        assertEquals("Get rollups response returned an unexpected number of jobs", DEFAULT_SIZE, resRollups.size)

        // Get rollups with a larger response size
        res = client().makeRequest("GET", "$ROLLUP_JOBS_BASE_URI?size=$rollupCount")
        map = res.asMap()
        resRollups = map["rollups"] as List<Map<String, Any?>>

        // There can be leftover rollups from previous tests, so we will have at least rollupCount or more
        assertEquals("Total rollups was not the same", rollupCount, resRollups.size)
    }

    @Throws(Exception::class)
    fun `test checking if a rollup exists`() {
        val rollup = createRandomRollup()

        val headResponse = client().makeRequest("HEAD", "$ROLLUP_JOBS_BASE_URI/${rollup.id}")
        assertEquals("Unable to HEAD rollup", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    @Throws(Exception::class)
    fun `test checking if a non-existent rollup exists`() {
        val headResponse = client().makeRequest("HEAD", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }
}
