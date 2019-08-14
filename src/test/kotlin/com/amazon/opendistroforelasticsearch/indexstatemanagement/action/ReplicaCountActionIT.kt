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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomDefaultNotification
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ReplicaCountActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic replica count`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val actionConfig = ReplicaCountActionConfig(10, 0)
        val states = listOf(State(name = "ReplicaCountState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            defaultNotification = randomDefaultNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        // create index defaults to 1 replica
        createIndex(indexName, policyID)

        assertEquals("Index did not default to 1 replica", 1, getNumberOfReplicasSetting(indexName))

        // give time for coordinator to create managed index job
        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Need to speed up to second execution where it will trigger the first execution of the action which
        // should set the replica count to the desired number
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        val historySearchResponse = getHistorySearchResponse(indexName)
        assertEquals(3, historySearchResponse.hits.totalHits.value)
        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
            indexName,
            getUuid(indexName),
            policyID,
            0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("ReplicaCountState", actualHistory.stateMetaData!!.startTime),
            actionMetaData = ActionMetaData(ActionConfig.ActionType.REPLICA_COUNT.toString(), actualHistory.actionMetaData!!.startTime, 0, false, 0, 0),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Set number_of_replicas to 10")
        )

        assertEquals(expectedHistory, actualHistory)

        assertEquals("Index did not set number_of_replicas to ${actionConfig.numOfReplicas}", actionConfig.numOfReplicas, getNumberOfReplicasSetting(indexName))
    }
}
