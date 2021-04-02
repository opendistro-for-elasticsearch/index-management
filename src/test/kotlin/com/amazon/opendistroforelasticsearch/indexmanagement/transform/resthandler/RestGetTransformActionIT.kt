package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SIZE
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.junit.annotations.TestLogging
import org.elasticsearch.test.rest.ESRestTestCase.randomList

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestGetTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test getting a transform`() {
        var transform = createTransform(randomTransform())
        val indexedTransform = getTransform(transform.id)

        transform = transform.copy(
            schemaVersion = indexedTransform.schemaVersion,
            updatedAt = indexedTransform.updatedAt,
            jobSchedule = indexedTransform.jobSchedule
        )
        assertEquals("Indexed and retrieved transform differ", transform, indexedTransform)
    }

    @Throws(Exception::class)
    fun `test getting a transform that doesn't exist`() {
        try {
            getTransform(ESTestCase.randomAlphaOfLength(20))
            fail("Expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting all transforms`() {
        val transforms = randomList(1, 15) { createTransform(randomTransform()) }

        // TODO: Delete existing transforms before test once delete API is available
        // Using a larger response size than the default in case leftover transforms prevent the ones created in this test from being returned
        val res = client().makeRequest("GET", "$TRANSFORM_BASE_URI?size=100")
        val map = res.asMap()
        val totalTransforms = map["total_transforms"] as Int
        val resTransforms = map["transforms"] as List<Map<String, Any?>>

        // There can be leftover transforms from previous tests, so we will have at least transforms.size or more
        assertTrue("Total transforms was not the same", transforms.size <= totalTransforms)
        assertTrue("Transform response has different size", transforms.size <= resTransforms.size)
        for (testTransform in transforms) {
            val foundTransform = resTransforms.find { testTransform.id == it["_id"] as String }
            assertNotNull("Did not find matching transform that should exist", foundTransform)
            val innerTransform = foundTransform!!["transform"] as Map<String, Any?>
            assertEquals(testTransform.id, foundTransform["_id"] as String)
            assertEquals(testTransform.seqNo, (foundTransform["_seq_no"] as Int).toLong())
            assertEquals(testTransform.primaryTerm, (foundTransform["_primary_term"] as Int).toLong())
            assertEquals(testTransform.id, innerTransform["transform_id"] as String)
            assertEquals(7, (innerTransform["schema_version"] as Int).toLong())
            assertEquals(testTransform.enabled, innerTransform["enabled"] as Boolean)
            assertEquals(testTransform.enabledAt?.toEpochMilli(), (innerTransform["enabled_at"] as Number?)?.toLong())

            assertEquals(testTransform.description, innerTransform["description"] as String)
            assertEquals(testTransform.sourceIndex, innerTransform["source_index"] as String)
            assertEquals(testTransform.targetIndex, innerTransform["target_index"] as String)
            assertEquals(testTransform.roles, innerTransform["roles"] as List<String>)
            assertEquals(testTransform.pageSize, innerTransform["page_size"] as Int)
            assertEquals(testTransform.groups.size, (innerTransform["groups"] as List<Dimension>).size)
        }
    }

    @Throws(Exception::class)
    fun `test changing response size when getting transforms`() {
        val transformCount = 25
        repeat(transformCount) { createTransform(randomTransform()) }

        var res = client().makeRequest("GET", TRANSFORM_BASE_URI)
        var map = res.asMap()
        var resTransforms = map["transforms"] as List<Map<String, Any?>>

        assertEquals("Get transforms response returned an unexpected number of transforms", DEFAULT_SIZE, resTransforms.size)

        // Get Transforms with a larger response size
        res = client().makeRequest("GET", "$TRANSFORM_BASE_URI?size=$transformCount")
        map = res.asMap()
        resTransforms = map["transforms"] as List<Map<String, Any?>>

        assertEquals("Total transforms was not the same", transformCount, resTransforms.size)
    }

    @Throws(Exception::class)
    fun `test checking if a transform exists`() {
        val transform = createRandomTransform()
        val headResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Unable to HEAD transform", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    @Throws(Exception::class)
    fun `test checking if a non-existent transform exists`() {
        val headResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }
}
