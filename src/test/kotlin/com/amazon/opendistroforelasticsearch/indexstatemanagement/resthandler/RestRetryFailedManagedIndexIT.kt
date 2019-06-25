package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestRetryFailedManagedIndex.Companion.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestRetryFailedManagedIndex.Companion.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestRetryFailedManagedIndex.Companion.UPDATED_INDICES
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import java.time.Instant

class RestRetryFailedManagedIndexIT : IndexStateManagementRestTestCase() {

    fun `test missing indices`() {
        try {
            client().makeRequest(RestRequest.Method.POST.toString(), RestRetryFailedManagedIndex.RETRY_BASE_URI)
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test index list`() {
        val indexName = "movies"
        val indexName1 = "movies_1"
        val indexName2 = "movies_2"
        val indexName3 = "some_other_test"
        createIndex(indexName, null)
        createIndex(indexName1, "somePolicy")
        createIndex(indexName2, null)
        createIndex(indexName3, null)

        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName,$indexName1")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUUID(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUUID(indexName1),
                    "reason" to "There is no index metadata information"
                )
            )
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexName = "movies"
        val indexName1 = "movies_1"
        val indexName2 = "movies_2"
        val indexName3 = "some_other_test"
        createIndex(indexName, null)
        createIndex(indexName1, null)
        createIndex(indexName2, "somePolicy")
        createIndex(indexName3, null)

        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName*")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUUID(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUUID(indexName1),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName2,
                    "index_uuid" to getUUID(indexName2),
                    "reason" to "There is no index metadata information"
                )
            )
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    fun `test index not being managed`() {
        val indexName = "movies"
        createIndex(indexName, null)
        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUUID(indexName),
                    "reason" to "This index is not being managed."
                )
            )
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    fun `test index has no metadata`() {
        val indexName = "movies"
        createIndex(indexName, "somePolicy")
        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUUID(indexName),
                    "reason" to "There is no index metadata information"
                )
            )
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    fun `test index not failed`() {
        val indexName = "movies"
        val policy = createRandomPolicy(refresh = true)
        createIndex(indexName, policyName = policy.id)

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUUID(indexName),
                    "reason" to "This index is not in failed state."
                )
            )
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    fun `test index failed`() {
        val indexName = "movies"
        createIndex(indexName, "invalid_policy")

        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        val response = client().makeRequest(RestRequest.Method.POST.toString(), "${RestRetryFailedManagedIndex.RETRY_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus.", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to false
        )
        assertRetryFailedManagedIndexResponse(expectedErrorMessage, actualMessage)
    }

    @Suppress("UNCHECKED_CAST")
    private fun getUUID(indexName: String): String {
        val indexSettings = getIndexSettings(indexName) as Map<String, Map<String, Map<String, Any?>>>
        return indexSettings[indexName]!!["settings"]!!["index.uuid"] as String
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertRetryFailedManagedIndexResponse(expected: Map<String, Any>, actual: Map<String, Any>) {
        for (entry in actual) {
            when (val value = entry.value) {
                is String -> assertEquals(expected[entry.key] as String, value)
                is Boolean -> assertEquals(expected[entry.key] as Boolean, value)
                is Int -> assertEquals(expected[entry.key] as Int, value)
                is List<*> -> {
                    // Assume at this point we are checking for failed_indices.
                    value as List<Map<String, String>>
                    val actualArray = value.toTypedArray()
                    actualArray.sortWith(compareBy({ it["index_name"] }))
                    val expectedArray = (actual[entry.key] as List<Map<String, String>>).toTypedArray()
                    expectedArray.sortWith(compareBy({ it["index_name"] }))
                    assertArrayEquals(expectedArray, actualArray)
                }
                else -> fail("unknown type $value")
            }
        }
    }
}
