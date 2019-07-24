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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomSweptManagedIndexConfig
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase

class ManagedIndexUtilsTests : ESTestCase() {

    fun `test create managed index request`() {
        val index = randomAlphaOfLength(10)
        val uuid = randomAlphaOfLength(10)
        val policyID = randomAlphaOfLength(10)
        val createRequest = createManagedIndexRequest(index, uuid, policyID)

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
        val clusterStateManagedIndexConfig = ClusterStateManagedIndexConfig(index = index, uuid = uuid, policyID = policyID)
        val updateRequest = updateManagedIndexRequest(clusterStateManagedIndexConfig)

        assertNotNull("UpdateRequest not created", updateRequest)
        assertEquals("Incorrect ism index used in request", INDEX_STATE_MANAGEMENT_INDEX, updateRequest.index())
        assertEquals("Incorrect uuid used as document id on request", uuid, updateRequest.id())

        val source = updateRequest.doc().sourceAsMap()
        assertEquals("Incorrect policy_id added to change_policy", clusterStateManagedIndexConfig.policyID,
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
            )
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

    @Suppress("UNCHECKED_CAST")
    fun `test get update managed index requests`() {
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

        val requests = getUpdateManagedIndexRequests(
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
        assertEquals("Incorrect uuid used as document id on request", clusterConfigToUpdate.uuid, request.id())
        assertTrue("Incorrect request type", request is UpdateRequest)
        val source = (request as UpdateRequest).doc().sourceAsMap()
        assertEquals("Incorrect policy_id added to change_policy", clusterConfigToUpdate.policyID,
                ((source["managed_index"] as Map<String, Any>)["change_policy"] as Map<String, String>)["policy_id"])
    }

    fun `test get swept managed index search request`() {
        val searchRequest = getSweptManagedIndexSearchRequest()

        val builder = searchRequest.source()
        val indices = searchRequest.indices().toList()
        assertTrue("Does not return seqNo and PrimaryTerm", builder.seqNoAndPrimaryTerm())
        assertEquals("Wrong index being searched", listOf(INDEX_STATE_MANAGEMENT_INDEX), indices)
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE,
                bytesReference, XContentType.JSON)
    }
}
