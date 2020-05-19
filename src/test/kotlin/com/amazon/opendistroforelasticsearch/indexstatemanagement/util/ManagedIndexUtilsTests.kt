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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Conditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomSweptManagedIndexConfig
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import java.time.Instant

class ManagedIndexUtilsTests : ESTestCase() {

    fun `test create managed index request`() {
        val index = randomAlphaOfLength(10)
        val uuid = randomAlphaOfLength(10)
        val policyID = randomAlphaOfLength(10)
        val createRequest = managedIndexConfigIndexRequest(index, uuid, policyID, 5)

        assertNotNull("IndexRequest not created", createRequest)
        assertEquals("Incorrect ism index used in request", INDEX_STATE_MANAGEMENT_INDEX, createRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, createRequest.id())

        val source = createRequest.source()
        val managedIndexConfig = ManagedIndexConfig.parseWithType(contentParser(source))
        assertEquals("Incorrect index on ManagedIndexConfig source", index, managedIndexConfig.index)
        assertEquals("Incorrect name on ManagedIndexConfig source", index, managedIndexConfig.name)
        assertEquals("Incorrect index uuid on ManagedIndexConfig source", uuid, managedIndexConfig.indexUuid)
        assertEquals("Incorrect policy_id on ManagedIndexConfig source", policyID, managedIndexConfig.policyID)
    }

    fun `test delete managed index request`() {
        val uuid = randomAlphaOfLength(10)
        val deleteRequest = deleteManagedIndexRequest(uuid)

        assertNotNull("deleteRequest not created", deleteRequest)
        assertEquals("Incorrect ism index used in request", INDEX_STATE_MANAGEMENT_INDEX, deleteRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, deleteRequest.id())
    }

    @Suppress("UNCHECKED_CAST")
    fun `test update managed index request`() {
        val index = randomAlphaOfLength(10)
        val uuid = randomAlphaOfLength(10)
        val policyID = randomAlphaOfLength(10)
        val sweptManagedIndexConfig = SweptManagedIndexConfig(index = index, uuid = uuid, policyID = policyID,
                primaryTerm = 1, seqNo = 1, changePolicy = randomChangePolicy(policyID = policyID), policy = null)
        val updateRequest = updateManagedIndexRequest(sweptManagedIndexConfig)

        assertNotNull("UpdateRequest not created", updateRequest)
        assertEquals("Incorrect ism index used in request", INDEX_STATE_MANAGEMENT_INDEX, updateRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, updateRequest.id())

        val source = updateRequest.doc().sourceAsMap()
        logger.info("source is $source")
        assertEquals("Incorrect policy_id added to change_policy", sweptManagedIndexConfig.policyID,
                ((source["managed_index"] as Map<String, Any>)["change_policy"] as Map<String, String>)["policy_id"])
    }

    fun `test get create managed index requests`() {
        val sweptConfigToDelete = randomSweptManagedIndexConfig(policyID = "delete_me")

        val clusterConfigToCreate = randomClusterStateManagedIndexConfig(policyID = "some_policy")

        val clusterConfigToUpdate = randomClusterStateManagedIndexConfig(policyID = "update_me")
        val sweptConfigToBeUpdated = randomSweptManagedIndexConfig(index = clusterConfigToUpdate.index,
                uuid = clusterConfigToUpdate.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17)

        val clusterConfigBeingUpdated = randomClusterStateManagedIndexConfig(policyID = "updating")
        val sweptConfigBeingUpdated = randomSweptManagedIndexConfig(index = clusterConfigBeingUpdated.index,
                uuid = clusterConfigBeingUpdated.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17,
                changePolicy = randomChangePolicy("updating"))

        val clusterConfig = randomClusterStateManagedIndexConfig(policyID = "do_nothing")
        val sweptConfig = randomSweptManagedIndexConfig(index = clusterConfig.index,
                uuid = clusterConfig.uuid, policyID = clusterConfig.policyID, seqNo = 5, primaryTerm = 17)

        val requests = getCreateManagedIndexRequests(
            mapOf(
                clusterConfigToCreate.uuid to clusterConfigToCreate,
                clusterConfigToUpdate.uuid to clusterConfigToUpdate,
                clusterConfig.uuid to clusterConfig,
                clusterConfigBeingUpdated.uuid to clusterConfigBeingUpdated
            ),
            mapOf(
                sweptConfig.uuid to sweptConfig,
                sweptConfigToDelete.uuid to sweptConfigToDelete,
                sweptConfigToBeUpdated.uuid to sweptConfigToBeUpdated,
                sweptConfigBeingUpdated.uuid to sweptConfigBeingUpdated
            ),
            5
        )

        assertEquals("Too many requests", 1, requests.size)
        val request = requests.first()
        assertEquals("Incorrect uuid used as document id on request", clusterConfigToCreate.uuid, request.id())
        assertTrue("Incorrect request type", request is IndexRequest)
        val source = (request as IndexRequest).source()
        val managedIndexConfig = ManagedIndexConfig.parseWithType(contentParser(source))

        assertEquals("Incorrect index on ManagedIndexConfig source", clusterConfigToCreate.index, managedIndexConfig.index)
        assertEquals("Incorrect name on ManagedIndexConfig source", clusterConfigToCreate.index, managedIndexConfig.name)
        assertEquals("Incorrect index uuid on ManagedIndexConfig source", clusterConfigToCreate.uuid, managedIndexConfig.indexUuid)
        assertEquals("Incorrect policy_id on ManagedIndexConfig source", clusterConfigToCreate.policyID, managedIndexConfig.policyID)
    }

    fun `test get delete managed index requests`() {
        val sweptConfigToDelete = randomSweptManagedIndexConfig(policyID = "delete_me")

        val clusterConfigToCreate = randomClusterStateManagedIndexConfig(policyID = "some_policy")

        val clusterConfigToUpdate = randomClusterStateManagedIndexConfig(policyID = "update_me")
        val sweptConfigToBeUpdated = randomSweptManagedIndexConfig(index = clusterConfigToUpdate.index,
                uuid = clusterConfigToUpdate.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17)

        val clusterConfigBeingUpdated = randomClusterStateManagedIndexConfig(policyID = "updating")
        val sweptConfigBeingUpdated = randomSweptManagedIndexConfig(index = clusterConfigBeingUpdated.index,
                uuid = clusterConfigBeingUpdated.uuid, policyID = "to_something_new", seqNo = 5, primaryTerm = 17,
                changePolicy = randomChangePolicy("updating"))

        val clusterConfig = randomClusterStateManagedIndexConfig(policyID = "do_nothing")
        val sweptConfig = randomSweptManagedIndexConfig(index = clusterConfig.index,
                uuid = clusterConfig.uuid, policyID = clusterConfig.policyID, seqNo = 5, primaryTerm = 17)

        val requests = getDeleteManagedIndexRequests(
            mapOf(
                clusterConfigToCreate.uuid to clusterConfigToCreate,
                clusterConfigToUpdate.uuid to clusterConfigToUpdate,
                clusterConfig.uuid to clusterConfig,
                clusterConfigBeingUpdated.uuid to clusterConfigBeingUpdated
            ),
            mapOf(
                sweptConfig.uuid to sweptConfig,
                sweptConfigToDelete.uuid to sweptConfigToDelete,
                sweptConfigToBeUpdated.uuid to sweptConfigToBeUpdated,
                sweptConfigBeingUpdated.uuid to sweptConfigBeingUpdated
            )
        )

        assertEquals("Too many requests", 1, requests.size)
        val request = requests.first()
        assertEquals("Incorrect uuid used as document id on request", sweptConfigToDelete.uuid, request.id())
        assertTrue("Incorrect request type", request is DeleteRequest)
    }

    fun `test get swept managed index search request`() {
        val searchRequest = getSweptManagedIndexSearchRequest()

        val builder = searchRequest.source()
        val indices = searchRequest.indices().toList()
        assertTrue("Does not return seqNo and PrimaryTerm", builder.seqNoAndPrimaryTerm())
        assertEquals("Wrong index being searched", listOf(INDEX_STATE_MANAGEMENT_INDEX), indices)
    }

    fun `test rollover action config evaluate conditions`() {
        val noConditionsConfig = RolloverActionConfig(minSize = null, minDocs = null, minAge = null, index = 0)
        assertTrue("No conditions should always pass", noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue(0)))
        assertTrue("No conditions should always pass", noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(100), numDocs = 5, indexSize = ByteSizeValue(5)))
        // assertTrue("No conditions should always pass", noConditionsConfig
        //         .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(-6000), numDocs = 5, indexSize = ByteSizeValue(5)))
        assertTrue("No conditions should always pass", noConditionsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(6000), numDocs = 5, indexSize = ByteSizeValue(5)))

        val minSizeConfig = RolloverActionConfig(minSize = ByteSizeValue(5), minDocs = null, minAge = null, index = 0)
        assertFalse("Less bytes should not pass", minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO))
        assertTrue("Equal bytes should pass", minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(5)))
        assertTrue("More bytes should pass", minSizeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue(10)))

        val minDocsConfig = RolloverActionConfig(minSize = null, minDocs = 5, minAge = null, index = 0)
        assertFalse("Less docs should not pass", minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO))
        assertTrue("Equal docs should pass", minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 5, indexSize = ByteSizeValue.ZERO))
        assertTrue("More docs should pass", minDocsConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 10, indexSize = ByteSizeValue.ZERO))

        val minAgeConfig = RolloverActionConfig(minSize = null, minDocs = null, minAge = TimeValue.timeValueSeconds(5), index = 0)
        assertFalse("Index age that is too young should not pass", minAgeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(1000), numDocs = 0, indexSize = ByteSizeValue.ZERO))
        assertTrue("Index age that is older should pass", minAgeConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(10000), numDocs = 0, indexSize = ByteSizeValue.ZERO))

        val multiConfig = RolloverActionConfig(minSize = ByteSizeValue(1), minDocs = 1, minAge = TimeValue.timeValueSeconds(5), index = 0)
        assertFalse("No conditions met should not pass", multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue.ZERO))
        assertTrue("Multi condition, age should pass", multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(10000), numDocs = 0, indexSize = ByteSizeValue.ZERO))
        assertTrue("Multi condition, docs should pass", multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 2, indexSize = ByteSizeValue.ZERO))
        assertTrue("Multi condition, size should pass", multiConfig
                .evaluateConditions(indexAgeTimeValue = TimeValue.timeValueMillis(0), numDocs = 0, indexSize = ByteSizeValue(2)))
    }

    fun `test transition evaluate conditions`() {
        val emptyTransition = Transition(stateName = "some_state", conditions = null)
        assertTrue("No conditions should pass", emptyTransition
                .evaluateConditions(indexCreationDate = Instant.now(), numDocs = null, indexSize = null, transitionStartTime = Instant.now()))

        val timeTransition = Transition(stateName = "some_state",
                conditions = Conditions(indexAge = TimeValue.timeValueSeconds(5), docCount = null, size = null, cron = null))
        assertFalse("Index age that is too young should not pass", timeTransition
                .evaluateConditions(indexCreationDate = Instant.now(), numDocs = null, indexSize = null, transitionStartTime = Instant.now()))
        assertTrue("Index age that is older should pass", timeTransition
                .evaluateConditions(indexCreationDate = Instant.now().minusSeconds(10), numDocs = null, indexSize = null, transitionStartTime = Instant.now()))
        assertFalse("Index age that is -1L should not pass", timeTransition
                .evaluateConditions(indexCreationDate = Instant.ofEpochMilli(-1L), numDocs = null, indexSize = null, transitionStartTime = Instant.now()))
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE,
                bytesReference, XContentType.JSON)
    }
}
