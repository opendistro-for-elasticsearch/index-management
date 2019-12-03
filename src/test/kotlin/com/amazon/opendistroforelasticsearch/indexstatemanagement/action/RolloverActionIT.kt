/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.rest.RestRequest
import org.junit.Assert
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RolloverActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test rollover multi condition byte size`() {
        val aliasName = "${testIndexName}_byte_alias"
        val indexNameBase = "${testIndexName}_index_byte"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_byte_1"
        val actionConfig = RolloverActionConfig(ByteSizeValue(10, ByteSizeUnit.BYTES), 1000000, null, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        // create index defaults
        createIndex(firstIndex, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action which
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Index rollover before it met the condition.", mapOf("message" to "Attempting to rollover"), getExplainManagedIndexMetaData(firstIndex).info) }

        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/1111",
            StringEntity("{ \"testkey\": \"some valueaaaaaaa\" }", ContentType.APPLICATION_JSON)
        )
        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/2222",
            StringEntity("{ \"testkey1\": \"some value1\" }", ContentType.APPLICATION_JSON)
        )
        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/3333",
            StringEntity("{ \"testkey2\": \"some value2\" }", ContentType.APPLICATION_JSON)
        )

        // Need to speed up to second execution where it will trigger the first execution of the action which
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Index did not rollover.", mapOf("message" to "Rolled over index"), getExplainManagedIndexMetaData(firstIndex).info) }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    fun `test rollover multi condition doc size`() {
        val aliasName = "${testIndexName}_doc_alias"
        val indexNameBase = "${testIndexName}_index_doc"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_doc_1"
        val actionConfig = RolloverActionConfig(ByteSizeValue(10, ByteSizeUnit.TB), 3, null, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        // create index defaults
        createIndex(firstIndex, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action which
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Index rollover before it met the condition.", mapOf("message" to "Attempting to rollover"), getExplainManagedIndexMetaData(firstIndex).info) }

        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/1111",
            StringEntity("{ \"testkey\": \"some value\" }", ContentType.APPLICATION_JSON)
        )
        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/2222",
            StringEntity("{ \"testkey1\": \"some value1\" }", ContentType.APPLICATION_JSON)
        )
        client().makeRequest(
            RestRequest.Method.PUT.toString(),
            "$firstIndex/_doc/3333",
            StringEntity("{ \"testkey2\": \"some value2\" }", ContentType.APPLICATION_JSON)
        )

        // Need to speed up to second execution where it will trigger the first execution of the action which
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Index did not rollover.", mapOf("message" to "Rolled over index"), getExplainManagedIndexMetaData(firstIndex).info) }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }
}
