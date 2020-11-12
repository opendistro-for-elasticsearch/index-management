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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class CloseActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = CloseActionConfig(0)
        val states = listOf(
            State("CloseState", listOf(actionConfig), listOf())
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
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        assertEquals("open", getIndexState(indexName))

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("close", getIndexState(indexName)) }
    }

    fun `test already closed index`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = CloseActionConfig(0)
        val states = listOf(
            State("CloseState", listOf(actionConfig), listOf())
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
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        closeIndex(indexName)
        assertEquals("close", getIndexState(indexName))

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("close", getIndexState(indexName)) }
    }

    fun `test transitioning a closed index`() {
        val indexName = "${testIndexName}_index_3"
        val policyID = "${testIndexName}_testPolicyName_3"
        val actionConfig = CloseActionConfig(0)
        val secondState = State("LastState", emptyList(), emptyList())
        val firstState = State("CloseState", listOf(actionConfig), listOf(Transition(stateName = secondState.name, conditions = null)))
        val states = listOf(firstState, secondState)

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
        createIndex(indexName, policyID)

        assertEquals("open", getIndexState(indexName))

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds and init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so the job will trigger in 2 seconds and close the index
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("close", getIndexState(indexName)) }

        // Change the start time so the job will trigger in 2 seconds and attempt transitions
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(secondState.name, getExplainManagedIndexMetaData(indexName).transitionTo) }

        // Change the start time so the job will trigger in 2 seconds and transition to next state (which should complete policy)
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(true, getExplainManagedIndexMetaData(indexName).policyCompleted) }
    }
}
