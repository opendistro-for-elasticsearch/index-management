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
            val rollup = randomRollup()
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI//_start", emptyMap(), rollup.toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
