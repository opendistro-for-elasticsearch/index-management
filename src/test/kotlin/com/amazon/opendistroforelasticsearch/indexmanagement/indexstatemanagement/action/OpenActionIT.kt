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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class OpenActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = OpenActionConfig(0)
        val states = listOf(
            State("OpenState", listOf(actionConfig), listOf())
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

        waitFor { assertEquals("open", getIndexState(indexName)) }

        waitForStepCompleted(indexName)
    }

    fun `test already open index`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = OpenActionConfig(0)
        val states = listOf(
            State("OpenState", listOf(actionConfig), listOf())
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

        waitFor { assertEquals("open", getIndexState(indexName)) }

        waitForStepCompleted(indexName)
    }
}
