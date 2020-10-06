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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.runner

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomTransition
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit

class ManagedIndexRunnerIT : IndexStateManagementRestTestCase() {

    fun `test version conflict fails job`() {
        val indexName = "version_conflict_index"
        val policyID = "version_conflict_policy"
        val actionConfig = OpenActionConfig(0)
        val states = listOf(State("OpenState", listOf(actionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$indexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy on managed index
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                strict = false
            )
        }

        // change policy seqNo on managed index
        updateManagedIndexConfigPolicySeqNo(managedIndexConfig.copy(policySeqNo = 17))

        // start execution to see if it moves to failed because of version conflict
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val expectedInfoString = mapOf("message" to "There is a version conflict between your previous execution and your managed index").toString()
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(
                    PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                        assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                )),
                getExplainMap(indexName),
                strict = false
            )
        }
    }

    fun `test job interval changing`() {
        val indexName = "job_interval_index_"

        val createdPolicy = createRandomPolicy()
        createIndex(indexName, createdPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        assertEquals("Created managed index did not default to 5 minutes", 5, (managedIndexConfig.jobSchedule as IntervalSchedule).interval)

        // init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(createdPolicy.id, getManagedIndexConfigByDocId(managedIndexConfig.id)?.policyID) }

        // change cluster job interval setting to 2 (minutes)
        updateClusterSetting(ManagedIndexSettings.JOB_INTERVAL.key, "2")

        // fast forward to next execution where at the end we should change the job interval time
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { (getManagedIndexConfigByDocId(managedIndexConfig.id)?.jobSchedule as? IntervalSchedule)?.interval == 2 }
    }

    fun `test allow list fails execution`() {
        val indexName = "allow_list_index"

        val firstState = randomState(name = "first_state", actions = listOf(randomReadOnlyActionConfig()),
            transitions = listOf(randomTransition(stateName = "second_state", conditions = null)))
        val secondState = randomState(name = "second_state", actions = listOf(randomReadWriteActionConfig()),
            transitions = listOf(randomTransition(stateName = "first_state", conditions = null)))
        val randomPolicy = randomPolicy(id = "allow_policy", states = listOf(firstState, secondState))

        val createdPolicy = createPolicy(randomPolicy, "allow_policy")
        createIndex(indexName, createdPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(createdPolicy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // speed up to first execution that should set index to read only
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(SetReadOnlyStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to second execution that should transition to second_state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondState.name), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to third execution that should set index back to read write
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(SetReadWriteStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to fourth execution that should transition to first_state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, firstState.name), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // remove read_only from the allowlist
        val allowedActions = ActionConfig.ActionType.values().toList()
            .filter { actionType -> actionType != ActionConfig.ActionType.READ_ONLY }
            .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
        updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)

        // speed up to fifth execution that should try to set index to read only and fail because the action is not allowed
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Attempted to execute action=read_only which is not allowed.", getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }
}
