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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.StateFilter
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestChangePolicyAction.Companion.INDEX_NOT_MANAGED
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestStatus
import org.junit.Before
import java.time.Instant

class RestChangePolicyActionIT : IndexStateManagementRestTestCase() {

    @Before
    fun setup() {
        createIndex("movies", null)
        createIndex("movies_1", null)
        createIndex("movies_2", null)
        createIndex("other_index", null)
    }

    fun `test missing index`() {
        try {
            client().makeRequest(RestRequest.Method.POST.toString(), RestChangePolicyAction.CHANGE_POLICY_BASE_URI)
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing index")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing index"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test nonexistent ism config index`() {
        try {
            val changePolicy = ChangePolicy("some_id", null, emptyList())
            client().makeRequest(RestRequest.Method.POST.toString(),
                    "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/other_index", emptyMap(), changePolicy.toHttpEntity())
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.NOT_FOUND, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf(
                            "type" to "index_not_found_exception",
                            "index_uuid" to "_na_",
                            "index" to ".opendistro-ism-config",
                            "resource.type" to "index_or_alias",
                            "resource.id" to ".opendistro-ism-config",
                            "reason" to "no such index [.opendistro-ism-config]"
                        )
                    ),
                    "type" to "index_not_found_exception",
                    "index_uuid" to "_na_",
                    "index" to ".opendistro-ism-config",
                    "resource.type" to "index_or_alias",
                    "resource.id" to ".opendistro-ism-config",
                    "reason" to "no such index [.opendistro-ism-config]"
                ),
                "status" to 404
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test nonexistent index`() {
        try {
            val changePolicy = ChangePolicy("some_id", null, emptyList())
            client().makeRequest(RestRequest.Method.POST.toString(),
                    "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/this_does_not_exist", emptyMap(), changePolicy.toHttpEntity())
            fail("Excepted a failure.")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus.", RestStatus.NOT_FOUND, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf(
                            "type" to "index_not_found_exception",
                            "index_uuid" to "_na_",
                            "index" to "this_does_not_exist",
                            "resource.type" to "index_or_alias",
                            "resource.id" to "this_does_not_exist",
                            "reason" to "no such index [this_does_not_exist]"
                        )
                    ),
                    "type" to "index_not_found_exception",
                    "index_uuid" to "_na_",
                    "index" to "this_does_not_exist",
                    "resource.type" to "index_or_alias",
                    "resource.id" to "this_does_not_exist",
                    "reason" to "no such index [this_does_not_exist]"
                ),
                "status" to 404
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test index not being managed`() {
        // Create a random policy to init .opendistro-ism-config index
        createRandomPolicy(refresh = true)
        val changePolicy = ChangePolicy("some_id", null, emptyList())
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/movies", emptyMap(), changePolicy.toHttpEntity())
        val expectedResponse = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to "movies",
                    "index_uuid" to getUuid("movies"),
                    "reason" to INDEX_NOT_MANAGED
                )
            ),
            UPDATED_INDICES to 0
        )
        assertAffectedIndicesResponseIsEqual(expectedResponse, response.asMap())
    }

    fun `test changing policy on an index that hasn't initialized yet`() {
        val policy = createRandomPolicy(refresh = true)
        val newPolicy = createPolicy(randomPolicy(), "new_policy", true)
        val (index) = createIndex("a_new_index", policy.id)

        Thread.sleep(5000)

        val managedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Managed index config is null", managedIndexConfig)
        assertNull("Change policy is not null", managedIndexConfig!!.changePolicy)
        assertNull("Policy has already initialized", managedIndexConfig.policy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should succeed
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList())
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity())

        assertAffectedIndicesResponseIsEqual(mapOf(FAILURES to false, FAILED_INDICES to emptyList<Any>(), UPDATED_INDICES to 1), response.asMap())

        Thread.sleep(5000)

        // speed up to first execution where we initialize the policy on the job
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // The initialized policy should be the change policy one
        val updatedManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Updated managed index config is null", updatedManagedIndexConfig)
        assertNull("Updated change policy is not null", updatedManagedIndexConfig!!.changePolicy)
        assertEquals("Initialized policyId is not the change policy id", newPolicy.id, updatedManagedIndexConfig.policyID)
        // Will use the unique generated description to ensure they are the same policies, the cached policy does not have
        // id, seqNo, primaryTerm on the policy itself so cannot directly compare
        // TODO: figure out why the newPolicy.lastUpdatedTime and cached policy lastUpdatedTime is off by a few milliseconds
        assertEquals("Initialized policy is not the change policy", newPolicy.description, updatedManagedIndexConfig.policy?.description)
    }

    fun `test changing policy on a valid index and log pattern`() {
        val policy = createRandomPolicy(refresh = true)
        val newPolicy = createPolicy(randomPolicy(), "new_policy", true)
        val (index) = createIndex("a_new_index", policy.id)

        Thread.sleep(5000)

        val managedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Managed index config is null", managedIndexConfig)
        assertNull("Change policy is not null", managedIndexConfig!!.changePolicy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should go through
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList())
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index,movi*", emptyMap(), changePolicy.toHttpEntity())
        val expectedResponse = mapOf(
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to "movies",
                    "index_uuid" to getUuid("movies"),
                    "reason" to INDEX_NOT_MANAGED
                ),
                mapOf(
                    "index_name" to "movies_1",
                    "index_uuid" to getUuid("movies_1"),
                    "reason" to INDEX_NOT_MANAGED
                ),
                mapOf(
                    "index_name" to "movies_2",
                    "index_uuid" to getUuid("movies_2"),
                    "reason" to INDEX_NOT_MANAGED
                )
            ),
            UPDATED_INDICES to 1
        )
        assertAffectedIndicesResponseIsEqual(expectedResponse, response.asMap())

        Thread.sleep(5000)

        val updatedManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Updated managed index config is null", updatedManagedIndexConfig)
        assertNotNull("Updated change policy is null", updatedManagedIndexConfig!!.changePolicy)
        assertEquals("Updated change policy policy id does not match", newPolicy.id, updatedManagedIndexConfig.changePolicy!!.policyID)
    }

    fun `test changing policy on an index in a state`() {
        // Creates a policy that has one state with one action (sets index to read only)
        val stateWithReadOnlyAction = randomState(actions = listOf(ReadOnlyActionConfig(index = 0)))
        val randomPolicy = randomPolicy(states = listOf(stateWithReadOnlyAction))
        val policy = createPolicy(randomPolicy, refresh = true)

        // Creates new policy that has two states, same as before except a second state with a delete action and a transition from readonly to delete states
        val stateWithDeleteAction = randomState(actions = listOf(DeleteActionConfig(index = 0)))
        val updatedStateWithReadOnlyAction = stateWithReadOnlyAction.copy(transitions = listOf(Transition(stateWithDeleteAction.name, null)))
        val newPolicy = createPolicy(randomPolicy(states = listOf(updatedStateWithReadOnlyAction, stateWithDeleteAction)), "new_policy", true)
        val (index) = createIndex("a_new_index", policy.id)

        // Set index to read-write
        updateIndexSettings(
            index,
            Settings.builder().put("index.blocks.write", false)
        )

        Thread.sleep(5000)

        val managedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Managed index config is null", managedIndexConfig)
        assertNull("Change policy is not null", managedIndexConfig!!.changePolicy)
        assertNull("Policy already initialized", managedIndexConfig.policy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        // speed up to first execution where we initialize the policy on the job
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // After first execution we should expect the change policy to still be null (since we haven't called it yet)
        // and the initial policy should of been cached
        val executedManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Executed managed index config is null", executedManagedIndexConfig)
        assertNull("Executed change policy is not null", executedManagedIndexConfig!!.changePolicy)
        assertNotNull("Executed policy is null", executedManagedIndexConfig.policy)
        assertEquals("Executed saved policy does not match initial policy", policy.id, executedManagedIndexConfig.policyID)
        assertEquals("Index writes should not be blocked", "false", getIndexBlocksWriteSetting(index))

        // We should expect the explain API to show an initialized ManagedIndexMetaData with the default state from the initial policy
        val explainResponse = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$index")

        assertPredicatesOnMetaData(
            listOf(
                index to listOf(
                    ManagedIndexSettings.POLICY_ID.key to policy.id::equals,
                    ManagedIndexMetaData.INDEX to executedManagedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to executedManagedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to executedManagedIndexConfig.policyID::equals,
                    StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                        assertStateEquals(StateMetaData(policy.defaultState, Instant.now().toEpochMilli()), stateMetaDataMap)
                )
            ), explainResponse.asMap(), false)

        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList())
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity())
        val expectedResponse = mapOf(
                FAILURES to false,
                FAILED_INDICES to emptyList<Any>(),
                UPDATED_INDICES to 1
        )
        assertAffectedIndicesResponseIsEqual(expectedResponse, response.asMap())

        // speed up to second execution we will have a ChangePolicy but not be in Transitions yet
        // which means we should still execute the ReadOnlyAction
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        val nextManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Next managed index config is null", nextManagedIndexConfig)
        assertNotNull("Next change policy is null", nextManagedIndexConfig!!.changePolicy)
        assertNotNull("Next policy is null", nextManagedIndexConfig.policy)
        assertEquals("Next saved policy does not match initial policy", policy.id, nextManagedIndexConfig.policyID)
        assertEquals("Next change policy does not match new policy", newPolicy.id, nextManagedIndexConfig.changePolicy?.policyID)
        assertEquals("Index writes should be blocked", "true", getIndexBlocksWriteSetting(index))

        // We should expect the explain API to show us in the ReadOnlyAction
        val nextExplainResponse = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$index")
        assertPredicatesOnMetaData(
            listOf(
                index to listOf(
                    ManagedIndexSettings.POLICY_ID.key to policy.id::equals,
                    ManagedIndexMetaData.INDEX to executedManagedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to executedManagedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to executedManagedIndexConfig.policyID::equals,
                    StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(StateMetaData(policy.defaultState, Instant.now().toEpochMilli()), stateMetaDataMap),
                    ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(ActionMetaData(name = ActionConfig.ActionType.READ_ONLY.type, startTime = Instant.now().toEpochMilli(), index = 0,
                                    failed = false, consumedRetries = 0, lastRetryTime = null, actionProperties = null), actionMetaDataMap)
                )
            ), nextExplainResponse.asMap(), false)

        // speed up to third execution so that we move to Transitions action
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // speed up to fourth execution so that we change policy
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        val changedManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Changed managed index config is null", changedManagedIndexConfig)
        assertNull("Changed change policy is not null", changedManagedIndexConfig!!.changePolicy)
        assertNotNull("Changed policy is null", changedManagedIndexConfig.policy)
        assertEquals("Changed saved policy does not match new policy", newPolicy.id, changedManagedIndexConfig.policyID)
        assertEquals("Index writes should still be blocked", "true", getIndexBlocksWriteSetting(index))

        // We should expect the explain API to show us with the new policy
        val changedExplainResponse = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$index")
        assertPredicatesOnMetaData(
            listOf(
                index to listOf(
                    ManagedIndexSettings.POLICY_ID.key to newPolicy.id::equals,
                    ManagedIndexMetaData.INDEX to changedManagedIndexConfig.index::equals,
                    ManagedIndexMetaData.INDEX_UUID to changedManagedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to changedManagedIndexConfig.policyID::equals,
                    StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(StateMetaData(policy.defaultState, Instant.now().toEpochMilli()), stateMetaDataMap),
                    ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                            assertActionEquals(ActionMetaData(name = ActionConfig.ActionType.TRANSITION.type, startTime = Instant.now().toEpochMilli(), index = 0,
                                    failed = false, consumedRetries = 0, lastRetryTime = null, actionProperties = null), actionMetaDataMap)
                )
            ), changedExplainResponse.asMap(), false)
    }

    fun `test change policy API should only apply to indices in the state filter`() {
        val thirdState = randomState(actions = emptyList(), transitions = emptyList())
        val secondState = randomState(actions = emptyList(), transitions = listOf(Transition(thirdState.name, null)))
        val firstState = randomState(actions = emptyList(), transitions = listOf(Transition(secondState.name, null)))
        val policy = createPolicy(randomPolicy(states = listOf(firstState, secondState, thirdState)), "new_policy", true)
        val (firstIndex) = createIndex("first_index", policy.id)

        Thread.sleep(2000)

        val firstManagedIndexConfig = getManagedIndexConfig(firstIndex)
        assertNotNull("Managed index config is null", firstManagedIndexConfig)

        // speed up to first execution where we initialize the policy on the job
        updateManagedIndexConfigStartTime(
            firstManagedIndexConfig!!,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // speed up to second execution where we attempt transition which should succeed
        // and transitionTo should be set
        updateManagedIndexConfigStartTime(
            firstManagedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // speed up to second execution where we attempt transition which should succeed
        // and transitionTo should be set
        updateManagedIndexConfigStartTime(
            firstManagedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // speed up to third execution where we transition to the new state
        updateManagedIndexConfigStartTime(
            firstManagedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        // create second index
        val (secondIndex) = createIndex("second_index", policy.id)

        Thread.sleep(5000)

        val secondManagedIndexConfig = getManagedIndexConfig(secondIndex)
        assertNotNull("Managed index config is null", secondManagedIndexConfig)

        // speed up to first execution where we initialize the policy on the job
        updateManagedIndexConfigStartTime(
            secondManagedIndexConfig!!,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        val newPolicy = createRandomPolicy(refresh = true)
        val changePolicy = ChangePolicy(newPolicy.id, null, listOf(StateFilter(state = firstState.name)))
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$firstIndex,$secondIndex", emptyMap(), changePolicy.toHttpEntity())
        val expectedResponse = mapOf(
            FAILURES to false,
            FAILED_INDICES to emptyList<Any>(),
            UPDATED_INDICES to 1
        )
        assertAffectedIndicesResponseIsEqual(expectedResponse, response.asMap())

        Thread.sleep(5000)

        // The first managed index should not have a change policy added to it as it should of been filtered out from the states filter
        val nextFirstManagedIndexConfig = getManagedIndexConfig(firstIndex)
        assertNotNull("Next first managed index config is null", nextFirstManagedIndexConfig)
        assertNull("Next first change policy is not null", nextFirstManagedIndexConfig!!.changePolicy)

        // The second managed index should have a change policy added to it
        val nextSecondManagedIndexConfig = getManagedIndexConfig(secondIndex)
        assertNotNull("Next second managed index config is null", nextSecondManagedIndexConfig)
        assertNotNull("Next second change policy is null", nextSecondManagedIndexConfig!!.changePolicy)
    }

    fun `test starting from a specific state using change policy API`() {
        val policy = createRandomPolicy(refresh = true)
        val newPolicy = createPolicy(randomPolicy(), "new_policy", true)
        val (index) = createIndex("a_new_index", policy.id)

        Thread.sleep(5000)

        val managedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Managed index config is null", managedIndexConfig)
        assertNull("Change policy is not null", managedIndexConfig!!.changePolicy)
        assertNull("Policy has already initialized", managedIndexConfig.policy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should succeed
        val changePolicy = ChangePolicy(newPolicy.id, "some_other_state", emptyList())
        val response = client().makeRequest(RestRequest.Method.POST.toString(),
                "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity())

        assertAffectedIndicesResponseIsEqual(mapOf(FAILURES to false, FAILED_INDICES to emptyList<Any>(), UPDATED_INDICES to 1), response.asMap())

        Thread.sleep(5000)

        // speed up to first execution where we initialize the policy on the job
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

        Thread.sleep(5000)

        // The initialized policy should be the change policy one
        val updatedManagedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Updated managed index config is null", updatedManagedIndexConfig)
        assertNull("Updated change policy is not null", updatedManagedIndexConfig!!.changePolicy)
        assertEquals("Initialized policyId is not the change policy id", newPolicy.id, updatedManagedIndexConfig.policyID)
        // Will use the unique generated description to ensure they are the same policies, the cached policy does not have
        // id, seqNo, primaryTerm on the policy itself so cannot directly compare
        // TODO: figure out why the newPolicy.lastUpdatedTime and cached policy lastUpdatedTime is off by a few milliseconds
        assertEquals("Initialized policy is not the change policy", newPolicy.description, updatedManagedIndexConfig.policy?.description)

        // should expect to see us starting in the state mentioned in changepolicy
        val explainResponse = client().makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$index")
        assertPredicatesOnMetaData(
            listOf(
                index to listOf(
                    ManagedIndexMetaData.INDEX_UUID to updatedManagedIndexConfig.indexUuid::equals,
                    ManagedIndexMetaData.POLICY_ID to newPolicy.id::equals,
                    StateMetaData.STATE to fun(stateMetaDataMap: Any?): Boolean =
                            assertStateEquals(StateMetaData("some_other_state", Instant.now().toEpochMilli()), stateMetaDataMap)
                )
            ), explainResponse.asMap(), false)
    }
}
