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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.coordinator

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import java.time.Instant
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
            val response = client().makeRequest("GET", "/$INDEX_STATE_MANAGEMENT_INDEX/_mapping")
            val parserMap = createParser(XContentType.JSON.xContent(),
                response.entity.content).map() as Map<String, Map<String, Map<String, Any>>>
            val mappingsMap = parserMap[INDEX_STATE_MANAGEMENT_INDEX]?.getValue("mappings")!!

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
            assertFalse("ISM index created for invalid policies", indexExists(INDEX_STATE_MANAGEMENT_INDEX))
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
        updateManagedIndexConfigStartTime(
            managedIndexConfig,
            Instant.now().minusSeconds(58).toEpochMilli()
        )

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
}
