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
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestGetRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test getting a rollup`() {
        var rollup = createRollup(randomRollup())
        val indexedRollup = getRollup(rollup.id)
        // Schema version and last updated time are updated during the creation so we need to update the original too for comparison
        rollup = rollup.copy(schemaVersion = indexedRollup.schemaVersion, jobLastUpdatedTime = indexedRollup.jobLastUpdatedTime)
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

        val res = client().makeRequest("GET", ROLLUP_JOBS_BASE_URI)
        val map = res.asMap()
        val totalRollups = map["total_rollups"] as Int
        val resRollups = map["rollups"] as List<Map<String, Any?>>

        assertEquals("Total rollups was not the same", rollups.size, totalRollups)
        assertEquals("Rollups response has different size", rollups.size, resRollups.size)
        // Rollups in response will default to sorting my ID so first sort original rollups before comparing
        rollups.sortBy { it.id }
        for (i in 0 until rollups.size) {
            val resRollup = resRollups[i]
            val innerRollup = resRollup["rollup"] as Map<String, Any?>
            val rollup = rollups[i]
            assertEquals(rollup.id, resRollup["_id"] as String)
            assertEquals(rollup.seqNo, (resRollup["_seq_no"] as Int).toLong())
            assertEquals(rollup.primaryTerm, (resRollup["_primary_term"] as Int).toLong())
            assertEquals(rollup.id, innerRollup["rollup_id"] as String)
            // Doesn't matter what rollup sets, current system is at schema version 5
            assertEquals(5, (innerRollup["schema_version"] as Int).toLong())
            assertEquals(rollup.enabled, innerRollup["enabled"] as Boolean)
            assertEquals(rollup.enabledTime?.toEpochMilli(), innerRollup["enabled_time"] as Long?)
            // Last updated time will never be correct as it gets updated in the API call
            // assertEquals(rollup.lastUpdateTime.toEpochMilli(), innerRollup["last_updated_time"] as Long)
            assertEquals(rollup.continuous, innerRollup["continuous"] as Boolean)
            assertEquals(rollup.targetIndex, innerRollup["target_index"] as String)
            assertEquals(rollup.sourceIndex, innerRollup["source_index"] as String)
            assertEquals(rollup.metadataID, innerRollup["metadata_id"] as String?)
            assertEquals(rollup.roles, innerRollup["roles"] as List<String>)
            assertEquals(rollup.pageSize, innerRollup["page_size"] as Int)
            assertEquals(rollup.description, innerRollup["description"] as String)
            assertEquals(rollup.delay, (innerRollup["delay"] as Number?)?.toLong())
            assertEquals(rollup.metrics.size, (innerRollup["metrics"] as List<Map<String, Any?>>).size)
            assertEquals(rollup.dimensions.size, (innerRollup["dimensions"] as List<Map<String, Any?>>).size)
        }
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
