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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    private val policyID1 = "t1"
    private val policyID2 = "t2"

    fun `test add template with invalid index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf(" "), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = ismTemp), policyID1)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "Validation Failed: 1: index_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test add template with overlapping index pattern`() {
        try {
            val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())
            val ismTemp2 = ISMTemplate(listOf("lo*"), 100, randomInstant())
            createPolicy(randomPolicy(ismTemplate = ismTemp), policyID1)
            createPolicy(randomPolicy(ismTemplate = ismTemp2), policyID2)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()["error"] as Map<String, Any>
            val expectedReason = "new policy $policyID2 has an ism template with index pattern [lo*] matching existing policy templates policy [$policyID1] => [log*], please use a different priority than 100"
            assertEquals(expectedReason, actualMessage["reason"])
        }
    }

    fun `test ism template managing index`() {
        val indexName1 = "log-000001"
        val indexName2 = "log-000002"
        val indexName3 = "log-000003"
        val policyID = "${testIndexName}_testPolicyName_1"

        // need to specify policyID null, can remove after policyID deprecated
        createIndex(indexName1, null)

        val ismTemp = ISMTemplate(listOf("log*"), 100, randomInstant())

        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = ismTemp
        )
        createPolicy(policy, policyID)

        createIndex(indexName2, null)
        createIndex(indexName3, Settings.builder().put(INDEX_HIDDEN, true).build())

        waitFor { assertNotNull(getManagedIndexConfig(indexName2)) }

        // TODO uncomment in remove policy id
        // val managedIndexConfig = getExistingManagedIndexConfig(indexName2)
        // updateManagedIndexConfigStartTime(managedIndexConfig)
        // waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName2).policyID) }

        // only index create after template can be managed
        assertPredicatesOnMetaData(
            listOf(indexName1 to listOf(ManagedIndexSettings.POLICY_ID.key to fun(policyID: Any?): Boolean = policyID == null)),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName1))

        // hidden index will not be manage
        assertPredicatesOnMetaData(
            listOf(indexName1 to listOf(ManagedIndexSettings.POLICY_ID.key to fun(policyID: Any?): Boolean = policyID == null)),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName3))
    }
}
