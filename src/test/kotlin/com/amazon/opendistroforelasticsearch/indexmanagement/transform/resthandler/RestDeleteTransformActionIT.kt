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

        var transform = randomTransform()
        transform = transform.copy(enabled = false)
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
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
