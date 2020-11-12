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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.coordinator

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.forcemerge.WaitForForceMergeStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ManagedIndexCoordinatorIT : IndexStateManagementRestTestCase() {

    fun `test creating index with valid policy_id`() {
        val (index, policyID) = createIndex()
        waitFor {
            val managedIndexConfig = getManagedIndexConfig(index)
            assertNotNull("Did not create ManagedIndexConfig", managedIndexConfig)
            assertNotNull("Invalid policy_id used", policyID)
            assertEquals("Has incorrect policy_id", policyID, managedIndexConfig!!.policyID)
            assertEquals("Has incorrect index", index, managedIndexConfig.index)
            assertEquals("Has incorrect name", index, managedIndexConfig.name)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test creating index with valid policy_id creates ism index with correct mappings`() {
        createIndex()
        waitFor {
            val response = client().makeRequest("GET", "/$INDEX_MANAGEMENT_INDEX/_mapping")
            val parserMap = createParser(XContentType.JSON.xContent(),
                response.entity.content).map() as Map<String, Map<String, Map<String, Any>>>
            val mappingsMap = parserMap[INDEX_MANAGEMENT_INDEX]?.getValue("mappings")!!

            val expected = createParser(
                XContentType.JSON.xContent(),
                javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText())

            val expectedMap = expected.map()
            assertEquals("Mappings are different", expectedMap, mappingsMap)
        }
    }

    fun `test creating index with invalid policy_id`() {
        val indexOne = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val indexTwo = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val indexThree = randomAlphaOfLength(10).toLowerCase(Locale.ROOT)

        createIndex(indexOne, Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, " ").build())
        createIndex(indexTwo, Settings.builder().put(ManagedIndexSettings.POLICY_ID.key, "").build())
        createIndex(indexThree, Settings.builder().putNull(ManagedIndexSettings.POLICY_ID.key).build())

        waitFor {
            assertFalse("ISM index created for invalid policies", indexExists(INDEX_MANAGEMENT_INDEX))
        }
    }

    fun `test deleting index with policy_id`() {
        val (index) = createIndex(policyID = "some_policy")
        waitFor {
            val afterCreateConfig = getManagedIndexConfig(index)
            assertNotNull("Did not create ManagedIndexConfig", afterCreateConfig)
            deleteIndex(index)
        }

        waitFor {
            val afterDeleteConfig = getManagedIndexConfig(index)
            assertNull("Did not delete ManagedIndexConfig", afterDeleteConfig)
        }
    }

    fun `test managed index metadata is cleaned up after removing policy_id`() {
        val policyID = "some_policy"
        val (index) = createIndex(policyID = policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(index)

        // Speed up execution to initialize policy on job
        // Loading policy will fail but ManagedIndexMetaData will be updated
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Verify ManagedIndexMetaData contains information
        waitFor {
            assertPredicatesOnMetaData(
                listOf(index to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(index),
                false
            )
        }

        // Remove policy_id from index
        removePolicyFromIndex(index)

        // Verify ManagedIndexMetaData has been cleared
        // Only ManagedIndexSettings.POLICY_ID set to null should be left in explain output
        waitFor {
            assertPredicatesOnMetaData(
                listOf(index to listOf(ManagedIndexSettings.POLICY_ID.key to fun(policyID: Any?): Boolean = policyID == null)),
                getExplainMap(index),
                true
            )
        }
    }

    fun `test disabling and reenabling ism`() {
        val indexName = "test_disable_ism_index-000001"
        val policyID = "test_policy_1"

        // Create a policy with one State that performs rollover
        val rolloverActionConfig = RolloverActionConfig(index = 0, minDocs = 5, minAge = null, minSize = null)
        val states = listOf(State(name = "RolloverState", actions = listOf(rolloverActionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "$policyID description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, "some_alias")

        // Add 5 documents so rollover condition will succeed
        insertSampleData(indexName, docCount = 5)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds and init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // Expect the Explain API to show an initialized ManagedIndexMetaData with the default state from the policy
        waitFor { assertEquals(policy.defaultState, getExplainManagedIndexMetaData(indexName).stateMetaData?.name) }

        // Disable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "false")

        // Speed up to next execution where job should get disabled
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Confirm job was disabled
        val disabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getManagedIndexConfigByDocId(managedIndexConfig.id)
            assertNotNull("Could not find ManagedIndexConfig", config)
            assertEquals("ManagedIndexConfig was not disabled", false, config!!.enabled)
            config
        }

        // Speed up to next execution and confirm that Explain API still shows information of policy initialization
        updateManagedIndexConfigStartTime(disabledManagedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to "Successfully initialized policy: $policyID").toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexMetaData.INDEX to indexName::equals,
                        ManagedIndexMetaData.POLICY_ID to policyID::equals,
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ),
                getExplainMap(indexName),
                false
            )
        }

        // Re-enable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "true")

        // Confirm job was re-enabled
        val enabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getExistingManagedIndexConfig(indexName)
            assertEquals("ManagedIndexConfig was not re-enabled", true, config.enabled)
            config
        }

        // Speed up to next execution where the job should be rescheduled and the index rolled over
        updateManagedIndexConfigStartTime(enabledManagedIndexConfig)

        waitFor { assertEquals(AttemptRolloverStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }

    fun `test not disabling ism on unsafe step`() {
        val indexName = "test_safe_disable_ism"
        val policyID = "test_policy_1"

        // Create a policy with one State that performs force_merge and another State that deletes the index
        val forceMergeActionConfig = ForceMergeActionConfig(index = 0, maxNumSegments = 1)
        val deleteActionConfig = DeleteActionConfig(index = 0)
        val states = listOf(
            State(
                name = "ForceMergeState",
                actions = listOf(forceMergeActionConfig),
                transitions = listOf(Transition(stateName = "DeleteState", conditions = null))
            ),
            State(name = "DeleteState", actions = listOf(deleteActionConfig), transitions = listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$policyID description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        waitFor { assertTrue("Segment count for [$indexName] was less than expected", validateSegmentCount(indexName, min = 2)) }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Expect the Explain API to show an initialized ManagedIndexMetaData with the default state from the policy
        waitFor { assertEquals(policy.defaultState, getExplainManagedIndexMetaData(indexName).stateMetaData?.name) }

        // Second execution: Index is set to read-only for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Verify maxNumSegments is set in action properties when kicking off force merge
        waitFor {
            assertEquals(
                "maxNumSegments not set in ActionProperties",
                forceMergeActionConfig.maxNumSegments,
                getExplainManagedIndexMetaData(indexName).actionMetaData?.actionProperties?.maxNumSegments
            )
        }

        // Disable Index State Management
        updateClusterSetting(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.key, "false")

        // Fourth execution: WaitForForceMergeStep is not safe to disable on, so the job should not disable yet
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Confirm we successfully executed the WaitForForceMergeStep
        waitFor { assertEquals(WaitForForceMergeStep.getSuccessMessage(indexName),
            getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Confirm job was not disabled
        assertEquals("ManagedIndexConfig was disabled early", true, getExistingManagedIndexConfig(indexName).enabled)

        // Validate segments were merged
        assertTrue("Segment count for [$indexName] after force merge is incorrect", validateSegmentCount(indexName, min = 1, max = 1))

        // Fifth execution: Attempt transition, which is safe to disable on, so job should be disabled
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // Explain API info should still be that of the last executed Step
        waitFor { assertEquals(WaitForForceMergeStep.getSuccessMessage(indexName),
            getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // Confirm job was disabled
        val disabledManagedIndexConfig: ManagedIndexConfig = waitFor {
            val config = getExistingManagedIndexConfig(indexName)
            assertEquals("ManagedIndexConfig was not disabled", false, config.enabled)
            config
        }

        // Speed up to next execution to confirm Explain API still shows information of the last executed step (WaitForForceMergeStep)
        updateManagedIndexConfigStartTime(disabledManagedIndexConfig)

        waitFor {
            val expectedInfoString = mapOf("message" to WaitForForceMergeStep.getSuccessMessage(indexName)).toString()
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexMetaData.INDEX to indexName::equals,
                        ManagedIndexMetaData.POLICY_ID to policyID::equals,
                        ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                    )
                ),
                getExplainMap(indexName),
                false
            )
        }
    }
}
