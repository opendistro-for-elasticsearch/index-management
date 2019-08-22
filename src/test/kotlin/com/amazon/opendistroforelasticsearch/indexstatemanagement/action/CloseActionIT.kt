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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.CloseActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class CloseActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index1"
        val policyID = "${testIndexName}_testPolicyName1"
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
        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        assertEquals("close", getIndexState(indexName))
    }

    fun `test already closed index`() {
        val indexName = "${testIndexName}_index2"
        val policyID = "${testIndexName}_testPolicyName2"
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
        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        assertEquals("close", getIndexState(indexName))
    }
}
