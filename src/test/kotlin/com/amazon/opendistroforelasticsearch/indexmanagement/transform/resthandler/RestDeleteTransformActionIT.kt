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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestDeleteTransformActionIT : TransformRestTestCase() {
    @Throws(Exception::class)
    fun `test deleting a transform`() {
        val transform = randomTransform().copy(enabled = false)
        createTransform(transform, transform.id, refresh = true)

        val deleteResponse = client().makeRequest("DELETE",
            "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())
        val itemList = deleteResponse.asMap()["items"] as ArrayList<Map<String, Map<String, String>>>
        val deleteMap = itemList[0]["delete"]
        assertEquals("Expected successful delete: ${deleteResponse.asMap()}", "deleted", deleteMap?.get("result"))

        val getResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Deleted transform still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting an enabled transform`() {
        val transform = randomTransform().copy(enabled = true)
        createTransform(transform, transform.id, refresh = true)

        try {
            client().makeRequest("DELETE",
                "$TRANSFORM_BASE_URI/${transform.id}")
            fail("Expected an Exception")
        } catch (e: Exception) {
            assertEquals("Expected ElasticsearchStatusException", ResponseException::class, e::class)
        }
    }

    @Throws(Exception::class)
    fun `test deleting an enabled transform with force flag`() {
        val transform = randomTransform().copy(enabled = true)
        createTransform(transform, transform.id, refresh = true)

        client().makeRequest("DELETE",
                                 "$TRANSFORM_BASE_URI/${transform.id}?force=true")
        val getResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Deleted transform still exists", RestStatus.NOT_FOUND, getResponse.restStatus())

    }

    @Throws(Exception::class)
    fun `test deleting a transform that doesn't exist in exiting config index`() {
        createRandomTransform()
        val res = client().makeRequest("DELETE", "$TRANSFORM_BASE_URI/foobarbaz")
        assertEquals("Expected OK response", RestStatus.OK, res.restStatus())

        val itemList = res.asMap()["items"] as ArrayList<Map<String, Map<String, String>>>
        val deleteMap = itemList[0]["delete"]
        assertEquals("Expected bulk response result: ${res.asMap()}", "not_found", deleteMap?.get("result"))
    }

    @Throws(Exception::class)
    fun `test deleting a transform that doesn't exist and config index doesn't exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            val res = client().makeRequest("DELETE", "$TRANSFORM_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException: ${res.asMap()}")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
