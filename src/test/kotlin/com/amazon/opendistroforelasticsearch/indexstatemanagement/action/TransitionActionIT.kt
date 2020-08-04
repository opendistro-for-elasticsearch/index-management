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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Conditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.transition.AttemptTransitionStep
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class TransitionActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test doc count condition`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val secondStateName = "second"
        val states = listOf(
            State("first", listOf(), listOf(Transition(secondStateName, Conditions(docCount = 5L)))),
            State(secondStateName, listOf(), listOf())
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

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initializing the policy/metadata
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Evaluating transition conditions for first time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should not have evaluated to true
        waitFor { assertEquals(AttemptTransitionStep.getEvaluatingMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Add 6 documents (>5)
        insertSampleData(indexName, 6)

        // Evaluating transition conditions for second time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should have evaluated to true
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondStateName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }
}