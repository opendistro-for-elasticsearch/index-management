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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import java.util.*

class ManagedIndexCoordinatorIT : IndexStateManagementRestTestCase() {

    fun `test creating index with valid policy_name`() {
        val (index, policyName) = createIndex()
        Thread.sleep(2000)
        val managedIndexConfig = getManagedIndexConfig(index)
        assertNotNull("Did not create ManagedIndexConfig", managedIndexConfig)
        assertNotNull("Invalid policy_name used", policyName)
        assertEquals("Has incorrect policy_name", policyName, managedIndexConfig!!.policyName)
        assertEquals("Has incorrect index", index, managedIndexConfig.index)
        assertEquals("Has incorrect name", index, managedIndexConfig.name)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test creating index with valid policy_name creates ism index with correct mappings`() {
        createIndex()
        Thread.sleep(2000)

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

    fun `test creating index with invalid policy_name`() {
        val indexOne = ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val indexTwo = ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        val indexThree = ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT)

        createIndex(indexOne, Settings.builder().put(ManagedIndexSettings.POLICY_NAME.key, " ").build())
        createIndex(indexTwo, Settings.builder().put(ManagedIndexSettings.POLICY_NAME.key, "").build())
        createIndex(indexThree, Settings.builder().putNull(ManagedIndexSettings.POLICY_NAME.key).build())

        Thread.sleep(2000)

        assertFalse("ISM index created for invalid policies", indexExists(INDEX_STATE_MANAGEMENT_INDEX))
    }

    fun `test deleting index with policy_name`() {
        val (index) = createIndex(policyName = "some_policy")
        Thread.sleep(2000)
        val afterCreateConfig = getManagedIndexConfig(index)
        assertNotNull("Did not create ManagedIndexConfig", afterCreateConfig)
        deleteIndex(index)
        Thread.sleep(2000)
        val afterDeleteConfig = getManagedIndexConfig(index)
        assertNull("Did not delete ManagedIndexConfig", afterDeleteConfig)
    }

    fun `test updating index with policy_name`() {
        val (indexValidToValid) = createIndex()
        val (indexValidToInvalid) = createIndex()
        val (indexInvalidToInvalid) = createIndex(policyName = " ")
        val (indexInvalidToValid) = createIndex(policyName = null)
        Thread.sleep(2000)
        updateIndexSettings(indexValidToValid, Settings.builder().put(ManagedIndexSettings.POLICY_NAME.key, "some_policy"))
        updateIndexSettings(indexValidToInvalid, Settings.builder().put(ManagedIndexSettings.POLICY_NAME.key, ""))
        updateIndexSettings(indexInvalidToInvalid, Settings.builder().putNull(ManagedIndexSettings.POLICY_NAME.key))
        updateIndexSettings(indexInvalidToValid, Settings.builder().put(ManagedIndexSettings.POLICY_NAME.key, "other_policy"))
        Thread.sleep(2000)
        assertEquals("Did not update managed index config",
                "some_policy", getManagedIndexConfig(indexValidToValid)?.changePolicy?.policyName)

        // TODO: This might change depending on hard vs soft delete
        assertNull("Did not delete managed index config", getManagedIndexConfig(indexValidToInvalid))

        assertNull("Created managed index config", getManagedIndexConfig(indexValidToInvalid))

        assertEquals("Did not create managed index config",
                "other_policy", getManagedIndexConfig(indexInvalidToValid)?.policyName)
    }
}
