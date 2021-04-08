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
class RestExplainTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test explain transform`() {
        val transform = randomTransform().copy(
            id = "test_explain_transform",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null,
            sourceIndex = "test_source",
            targetIndex = "test_target"
        ).let { createTransform(it, it.id) }
        createTransformSourceIndex(transform)
        updateTransformStartTime(transform)

        waitFor {
            val updatedTransform = getTransform(transformId = transform.id)
            assertNotNull("MetadataID on transform was null", updatedTransform.metadataId)
            val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/${updatedTransform.id}/_explain")
            assertEquals(RestStatus.OK, response.restStatus())
            val responseMap = response.asMap()
            assertNotNull("Response is null", responseMap)
            assertTrue("Response does not have metadata", responseMap.keys.isNotEmpty())
            val explainMetadata = responseMap[updatedTransform.id] as Map<String, Any>
            assertNotNull("Did not have key for transform ID", explainMetadata)
            assertEquals("Did not have metadata_id in explain response", updatedTransform.metadataId, explainMetadata["metadata_id"])
            val metadata = explainMetadata["transform_metadata"] as Map<String, Any>
            assertNotNull("Did not have metadata in explain response", metadata)
            // Not sure if this is true for transforms
            assertEquals("Status should be finished", TransformMetadata.Status.FINISHED.type, metadata["status"])
        }
    }

    @Throws(Exception::class)
    fun `test explain a transform with no id fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest("GET", "$TRANSFORM_BASE_URI//_explain", emptyMap(), transform.toHttpEntity())
            fail("Expected 400 BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test explain transform for nonexistent id`() {
        // Creating a transform so the config index exists
        createTransform(transform = randomTransform(), transformId = "doesnt_exist_some_other_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/doesnt_exist/_explain")
        assertNull("Nonexistent transform didn't return null", response.asMap()["doesnt_exist"])
    }

    @Throws(Exception::class)
    fun `test explain transform for wildcard id`() {
        // Creating a transform so the config index exists
        createTransform(transform = randomTransform(), transformId = "wildcard_some_id")
        createTransform(transform = randomTransform(), transformId = "wildcard_some_other_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/wildcard_some*/_explain")
        // We don't expect there to always be metadata we are creating random transforms and the job isn't running
        // but we do expect the wildcard some* to expand to the two jobs created above and have non-null values (meaning they exist)
        val map = response.asMap()
        assertNotNull("Non null wildcard_some_id value wasn't in the response", map["wildcard_some_id"])
        assertNotNull("Non null wildcard_some_other_id value wasn't in the response", map["wildcard_some_other_id"])
    }

    @Throws(Exception::class)
    fun `test explain transform for job that hasnt started`() {
        createTransform(transform = randomTransform().copy(metadataId = null), transformId = "not_started_some_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/not_started_some_id/_explain")
        val expectedMap = mapOf("not_started_some_id" to mapOf("metadata_id" to null, "transform_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain transform for metadata_id but no metadata`() {
        createTransform(transform = randomTransform().copy(metadataId = "some_metadata_id"), transformId = "no_meta_some_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_meta_some_id/_explain")
        val expectedMap = mapOf("no_meta_some_id" to mapOf("metadata_id" to "some_metadata_id", "transform_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain transform when config doesnt exist`() {
        val responseExplicit = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform/_explain")
        assertEquals("Non-existent transform didn't return null", mapOf("no_config_some_transform" to null), responseExplicit.asMap())

        val responseExplicitMultiple = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform,no_config_another_transform/_explain")
        assertEquals("Non-existent transform didn't return null", mapOf("no_config_some_transform" to null, "no_config_another_transform" to null), responseExplicitMultiple.asMap())

        val responseWildcard = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_another_*/_explain")
        assertEquals("Wildcard transform didn't return nothing", mapOf<String, Map<String, Any>?>(), responseWildcard.asMap())

        val responseMultipleTypes = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform,no_config_another_*/_explain")
        assertEquals("Non-existent and wildcard transform didn't return only non-existent as null", mapOf("no_config_some_transform" to null), responseMultipleTypes.asMap())
    }
}
