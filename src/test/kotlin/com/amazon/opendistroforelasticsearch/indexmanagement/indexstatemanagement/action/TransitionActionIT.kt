package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Conditions
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
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
        waitFor {
            assertEquals(
                AttemptTransitionStep.getEvaluatingMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Add 6 documents (>5)
        insertSampleData(indexName, 6)

        // Evaluating transition conditions for second time
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Should have evaluated to true
        waitFor {
            assertEquals(
                AttemptTransitionStep.getSuccessMessage(indexName, secondStateName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }
}