package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.util.Locale

class RestRetryFailedManagedIndexActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test missing indices`() {
        try {
            client().makeRequest(RestRequest.Method.POST.toString(), RestRetryFailedManagedIndexAction.RETRY_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
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
        val indexName = "${testIndexName}_movies"
        val indexName1 = "${indexName}_1"
        val indexName2 = "${indexName}_2"
        val indexName3 = "${testIndexName}_some_other_test"
        createIndex(indexName, null)
        createIndex(indexName1, "somePolicy")
        createIndex(indexName2, null)
        createIndex(indexName3, null)

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName,$indexName1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUuid(indexName1),
                    "reason" to "There is no IndexMetaData information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexName = "${testIndexName}_video"
        val indexName1 = "${indexName}_1"
        val indexName2 = "${indexName}_2"
        val indexName3 = "${testIndexName}_some_other_test_2"
        createIndex(indexName, null)
        createIndex(indexName1, null)
        createIndex(indexName2, "somePolicy")
        createIndex(indexName3, null)

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName*"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName1,
                    "index_uuid" to getUuid(indexName1),
                    "reason" to "This index is not being managed."
                ),
                mapOf(
                    "index_name" to indexName2,
                    "index_uuid" to getUuid(indexName2),
                    "reason" to "There is no IndexMetaData information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index not being managed`() {
        val indexName = "${testIndexName}_games"
        createIndex(indexName, null)
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "This index is not being managed."
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index has no metadata`() {
        val indexName = "${testIndexName}_players"
        createIndex(indexName, "somePolicy")
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedErrorMessage = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexName,
                    "index_uuid" to getUuid(indexName),
                    "reason" to "There is no IndexMetaData information"
                )
            )
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
    }

    fun `test index not failed`() {
        val indexName = "${testIndexName}_classic"
        val policy = createRandomPolicy(refresh = true)
        createIndex(indexName, policyID = policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        waitFor {
            val response = client().makeRequest(
                RestRequest.Method.POST.toString(),
                "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
            )
            assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
            val actualMessage = response.asMap()
            val expectedErrorMessage = mapOf(
                FAILURES to true,
                FAILED_INDICES to listOf(
                    mapOf(
                        "index_name" to indexName,
                        "index_uuid" to getUuid(indexName),
                        "reason" to "This index is not in failed state."
                    )
                )
            )
            assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
        }
    }

    fun `test index failed`() {
        val indexName = "${testIndexName}_blueberry"
        createIndex(indexName, "invalid_policy")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        waitFor {
            val response = client().makeRequest(
                RestRequest.Method.POST.toString(),
                "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
            )
            assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
            val actualMessage = response.asMap()
            val expectedErrorMessage = mapOf(
                UPDATED_INDICES to 1,
                FAILURES to false,
                FAILED_INDICES to emptyList<Map<String, Any>>()
            )
            assertAffectedIndicesResponseIsEqual(expectedErrorMessage, actualMessage)
        }
    }

    fun `test reset action start time`() {
        val indexName = "${testIndexName}_drewberry"
        val policyID = "${testIndexName}_policy_1"
        val policy = randomPolicy(states = listOf(randomState(actions = listOf(randomForceMergeActionConfig(maxNumSegments = 1)))))
        createPolicy(policy, policyId = policyID, refresh = true)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy on job
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        // verify we have policy
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                false
            )
        }

        // speed up to execute first action, readonly
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(ActionMetaData(name = "force_merge", startTime = Instant.now().toEpochMilli(), failed = false,
                                index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null), actionMetaDataMap)
                    )
                ), getExplainMap(indexName), false)
        }

        // close the index to cause next execution to fail
        closeIndex(indexName)

        // speed up to execute first action and fail, call force merge
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        // verify failed and save the startTime
        var firstStartTime: Long = Long.MAX_VALUE
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                            @Suppress("UNCHECKED_CAST")
                            actionMetaDataMap as Map<String, Any>
                            firstStartTime = actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long
                            return assertActionEquals(ActionMetaData(name = "force_merge", startTime = Instant.now().toEpochMilli(), failed = true,
                                index = 0, consumedRetries = 0, lastRetryTime = null, actionProperties = null), actionMetaDataMap)
                        }
                    )
                ), getExplainMap(indexName), false)
        }

        // retry
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$indexName"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expectedErrorMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to false,
            FAILED_INDICES to emptyList<Map<String, Any>>()
        )
        assertAffectedIndicesResponseIsEqual(expectedErrorMessage, response.asMap())

        // verify actionStartTime was reset to null
        assertPredicatesOnMetaData(
            listOf(
                indexName to listOf(
                    ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                        @Suppress("UNCHECKED_CAST")
                        actionMetaDataMap as Map<String, Any>
                        return actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long? == null
                    }
                )
            ), getExplainMap(indexName), false)

        // should execute and set the startTime again
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        // the new startTime should be greater than the first start time
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean {
                            @Suppress("UNCHECKED_CAST")
                            actionMetaDataMap as Map<String, Any>
                            return actionMetaDataMap[ManagedIndexMetaData.START_TIME] as Long > firstStartTime
                        }
                    )
                ), getExplainMap(indexName), false)
        }
    }
}
