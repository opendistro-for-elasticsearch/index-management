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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestStopTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test stopping a started transform`() {
        val transform = createTransform(randomTransform().copy(enabled = true, enabledAt = randomInstant(), metadataId = null))
        assertTrue("Transform was not enabled", transform.enabled)

        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedTransform.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a stopped Transform`() {
        val transform = createTransform(randomTransform().copy(enabled = true, enabledAt = randomInstant(), metadataId = null))
        assertTrue("Transform was not enabled", transform.enabled)

        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedTransform.enabled)

        val secondResponse = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        val updatedSecondTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedSecondTransform.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a finished transform`() {
        // Create a transform that finishes
        val transform = createTransform(
            randomTransform()
                .copy(
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    enabledAt = Instant.now(),
                    metadataId = null
                )
        )
        createTransformSourceIndex(transform)
        updateTransformStartTime(transform)

        // Assert it finished
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform never finished", TransformMetadata.Status.FINISHED, metadata.status)
            // Waiting for job to be disabled here to avoid version conflict exceptions later on
            assertFalse("Job was not disabled", updatedTransform.enabled)
        }

        // Try to stop a finished transform
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert it is still in finished status
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform should have stayed finished", TransformMetadata.Status.FINISHED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a failed transform`() {
        // Create a transform that will fail because no source index
        val transform = randomTransform().copy(
            id = "test_stopping_a_failed_transform",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null
        ).let { createTransform(it, it.id) }
        updateTransformStartTime(transform)

        // Assert its in failed
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform never failed", TransformMetadata.Status.FAILED, metadata.status)
        }

        // Stop transform
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert transform still failed status
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform should have stayed failed", TransformMetadata.Status.FAILED, metadata.status)
        }
    }

    /* Goes straight to finished before test can pick up STARTED status
    @Throws(Exception::class)
    fun `test stopping transform with metadata`() {
        generateNYCTaxiData("source")
        val transform = randomTransform().copy(
            id = "basic_term_query",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            updatedAt = Instant.now(),
            enabledAt = Instant.now(),
            description = "basic search test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataId = null,
            roles = emptyList(),
            pageSize = 10
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not STARTED", TransformMetadata.Status.STARTED, transformMetadata.status)

            // There are two calls to _stop happening serially which is prone to version conflicts during an ongoing job
            // so including it in a waitFor to ensure it can retry a few times
            val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
            assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
            val expectedResponse = mapOf("acknowledged" to true)
            assertEquals(expectedResponse, response.asMap())
        }

        val updatedTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedTransform.enabled)
        val transformMetadata = getTransformMetadata(updatedTransform.metadataId!!)
        assertEquals("Transform is not STOPPED", TransformMetadata.Status.STARTED, transformMetadata.status)
    }
     */

    @Throws(Exception::class)
    fun `test stop a transform with no id fails`() {
        try {
            client().makeRequest("POST", "$TRANSFORM_BASE_URI//_stop")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
