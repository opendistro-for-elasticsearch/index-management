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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.AllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import org.junit.Assume
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class AllocationActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = AllocationActionConfig(require = mapOf("box_type" to "hot"), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(
            State("Allocate", listOf(actionConfig), listOf())
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

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.require.box_type"))
            assertEquals(actionConfig.require["box_type"], settings["index.routing.allocation.require.box_type"])
        }
    }

    fun `test allocate require`() {
        Assume.assumeTrue(isMultiNode)

        val indexName = "${testIndexName}_multinode_require"
        val policyID = "${testIndexName}_multinode_require"
        val actionConfig = AllocationActionConfig(require = mapOf("_name" to "integTest-1"), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(
                State("Allocate", listOf(actionConfig), listOf())
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
        createIndex(indexName, policyID, null, "0")
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.require._name"))
            assertEquals(actionConfig.require["_name"], settings["index.routing.allocation.require._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals("integTest-1", getIndexShardNodes(indexName)[0])
        }
    }

    fun `test allocate exclude`() {
        Assume.assumeTrue(isMultiNode)

        val indexName = "${testIndexName}_multinode_exclude"
        val policyID = "${testIndexName}_multinode_exclude"
        val actionConfig = AllocationActionConfig(require = emptyMap(), exclude = mapOf("_name" to "integTest-0"), include = emptyMap(), index = 0)
        val states = listOf(
                State("Allocate", listOf(actionConfig), listOf())
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
        createIndex(indexName, policyID, null, "0")
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.exclude._name"))
            assertEquals(actionConfig.exclude["_name"], settings["index.routing.allocation.exclude._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertNotEquals("integTest-0", getIndexShardNodes(indexName)[0])
        }
    }

    fun `test allocate include`() {
        Assume.assumeTrue(isMultiNode)

        val indexName = "${testIndexName}_multinode_include"
        val policyID = "${testIndexName}_multinode_include"
        val actionConfig = AllocationActionConfig(require = emptyMap(), exclude = emptyMap(), include = mapOf("_name" to "integTest-1"), index = 0)
        val states = listOf(
                State("Allocate", listOf(actionConfig), listOf())
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
        createIndex(indexName, policyID, null, "0")
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            val settings = getFlatSettings(indexName)
            assertTrue(settings.containsKey("index.routing.allocation.include._name"))
            assertEquals(actionConfig.include["_name"], settings["index.routing.allocation.include._name"])
        }

        // Third execution: Waits for allocation to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertEquals("integTest-1", getIndexShardNodes(indexName)[0])
        }
    }
}
