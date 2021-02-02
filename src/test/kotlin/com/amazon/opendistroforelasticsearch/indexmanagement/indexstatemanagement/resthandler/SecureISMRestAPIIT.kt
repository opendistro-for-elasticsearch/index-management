/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.commons.rest.SecureRestClientBuilder
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.toJsonString
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.elasticsearch.rest.RestStatus
import org.junit.After
import org.junit.Before
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class SecureISMRestAPIIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    val user = "userOne"
    var userClient: RestClient? = null

    @Before
    fun create() {
        if (!isHttps()) return

        if (userClient == null) {
            createUser(user, user, arrayOf())
            userClient =
                SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, user).setSocketTimeout(60000)
                    .build()
        }
    }

    @After
    fun cleanup() {
        if (!isHttps()) return

        userClient?.close()
        deleteUser(user)
    }

    fun `test ism blocked api`() {
        if (!isHttps()) return

        createRolesMapping(arrayOf(user), "ism_read_access")
        val policy = randomPolicy()

        try {
            userClient?.makeRequest(
                "PUT",
                "${IndexManagementPlugin.POLICY_BASE_URI}/${policy.id}",
                emptyMap(),
                StringEntity(policy.toJsonString(), ContentType.APPLICATION_JSON)
            )
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.FORBIDDEN, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf(
                            "type" to "security_exception",
                            "reason" to "no permissions for [cluster:admin/opendistro/ism/policy/write] and User [name=$user, backend_roles=[], requestedTenant=null]"
                        )
                    ),
                    "type" to "security_exception",
                    "reason" to "no permissions for [cluster:admin/opendistro/ism/policy/write] and User [name=$user, backend_roles=[], requestedTenant=null]"
                ),
                "status" to 403
            )
            assertEquals(expectedErrorMessage, actualMessage)
        } finally {
            deleteRolesMapping("ism_read_access")
        }
    }

    fun `test ism blocked action`() {
        if (!isHttps()) return

        createRolesMapping(arrayOf(user), "ism_full_access")

        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = DeleteActionConfig(0)
        val states = listOf(
            State("DeleteState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createIndex(indexName, policyID = null)
        val res1 = userClient?.let { createPolicy(policy, policyID, client = it) }
        userClient?.let { addPolicyToIndex(indexName, policyID, it) }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals(
                mapOf(
                    "cause" to "no permissions for [indices:admin/delete] and associated roles [ism_full_access, own_index]",
                    "message" to "Failed to delete index [index=$indexName]"
                ), getExplainManagedIndexMetaData(indexName).info
            )
        }

        deleteRolesMapping("ism_full_access")
    }
}