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
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestIndexTransformActionIT : TransformRestTestCase() {
    // TODO: Once GET API written, add update transform tests (required for createTransform helper)

    @Throws(Exception::class)
    fun `test creating a transform`() {
        val transform = randomTransform()
        val response = client().makeRequest(
            "PUT",
            "$TRANSFORM_BASE_URI/${transform.id}",
            emptyMap(),
            transform.toHttpEntity()
            )
        assertEquals("Create transform failed", RestStatus.CREATED, response.restStatus())
        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        assertNotEquals("Response is missing Id", Transform.NO_ID, createdId)
        assertEquals("Not same id", transform.id, createdId)
        assertEquals("Incorrect Location header", "$TRANSFORM_BASE_URI/$createdId", response.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test creating a transform with no id fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest(
                "PUT",
                TRANSFORM_BASE_URI,
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a transform with POST fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/some_transform",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }
}
