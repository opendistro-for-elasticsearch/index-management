package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.makeRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest

class RestGetPoliciesActionIT : IndexStateManagementRestTestCase() {

    fun `test get policies for missing indices`() {
        try {
            client().makeRequest(RestRequest.Method.GET.toString(), IndexManagementPlugin.POLICY_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "reason" to "no such index [.opendistro-ism-config]",
                    "index_uuid" to "_na_",
                    "index" to ".opendistro-ism-config",
                    "resource.type" to "index_or_alias",
                    "type" to "index_not_found_exception",
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf(
                            "reason" to "no such index [.opendistro-ism-config]",
                            "index_uuid" to "_na_",
                            "index" to ".opendistro-ism-config",
                            "resource.type" to "index_or_alias",
                            "type" to "index_not_found_exception",
                            "resource.id" to ".opendistro-ism-config"
                        )
                    ),
                    "resource.id" to ".opendistro-ism-config"
                ),
                "status" to 404
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test get policies with actual policy`() {
        val policy = createRandomPolicy()

        val response = client().makeRequest(RestRequest.Method.GET.toString(), IndexManagementPlugin.POLICY_BASE_URI)

        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            "policies" to listOf<Map<String, Any>>(mapOf(
                "policy" to mapOf(
                    "seq_no" to policy.seqNo,
                    "schema_version" to policy.schemaVersion,
                    "policy_id" to policy.id,
                    "last_updated_time" to policy.lastUpdatedTime.toEpochMilli(),
                    "default_state" to policy.defaultState,
                    "description" to policy.description,
                    "error_notification" to policy.errorNotification,
                    "primary_term" to policy.primaryTerm,
                    "states" to policy.states.map {
                        mapOf(
                            "name" to it.name,
                            "transitions" to it.transitions,
                            "actions" to it.actions
                        )
                    }
                )
            )),
            "totalPolicies" to 1
        )

        assertEquals(expectedMessage.toString(), actualMessage.toString())
    }
}