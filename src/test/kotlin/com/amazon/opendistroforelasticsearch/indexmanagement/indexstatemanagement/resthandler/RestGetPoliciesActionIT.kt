package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.makeRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus

class RestGetPoliciesActionIT : IndexStateManagementRestTestCase() {

    fun `test get policies for missing indicies`() {
//        try {
//            val response = client().makeRequest(RestRequest.Method.GET.toString(), IndexManagementPlugin.POLICY_BASE_URI)
////            println(response.asMap())
//            fail("Expected a failure")
//        } catch (e: ResponseException) {
////            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
//            val actualMessage = e.response.asMap()
//            println(actualMessage["error"]["reason"])
//            val expectedErrorMessage = mapOf(
//                "error" to mapOf(
//                    "reason" to "nosuchindex[.opendistro-ism-config]",
//                    "index_uuid" to "_na_",
//                    "index" to ".opendistro-ism-config",
//                    "resource.type" to "index_or_alias",
//                    "type" to "index_not_found_exception",
//                    "root_cause" to listOf<Map<String, Any>>(
//                        mapOf(
//                            "reason" to "nosuchindex[.opendistro-ism-config]",
//                            "index_uuid" to "_na_",
//                            "index" to ".opendistro-ism-config",
//                            "resource.type" to "index_or_alias",
//                            "type" to "index_not_found_exception",
//                            "resource.id" to ".opendistro-ism-config"
//                        )
//                    ),
//                    "resource.id" to ".opendistro-ism-config"
//                ),
//                "status" to 404
//            )
//            assertEquals(expectedErrorMessage, actualMessage)
//        }
    }
}