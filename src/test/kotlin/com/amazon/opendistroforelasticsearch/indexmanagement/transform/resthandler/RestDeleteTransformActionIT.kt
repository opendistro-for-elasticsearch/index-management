package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestDeleteTransformActionIT : TransformRestTestCase() {
    @Throws(Exception::class)
    fun `test deleting a transform`() {
        val transform = createRandomTransform()

        val deleteResponse = client().makeRequest("DELETE",
            "$TRANSFORM_BASE_URI/${transform.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Deleted transform still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a transform that doesn't exist in exiting config index`() {
        createRandomTransform()
        val res = client().makeRequest("DELETE", "$TRANSFORM_BASE_URI/foobarbaz")
        assertEquals("Expected not_found response", "not_found", res.asMap()["result"])
    }

    @Throws(Exception::class)
    fun `test deleting a transform that doesn't exist and config index doesn't exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            val res = client().makeRequest("DELETE", "$TRANSFORM_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
