package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
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
}
