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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ForceMergeActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"

        // Create a Policy with one State that only preforms a force_merge Action
        val forceMergeActionConfig = ForceMergeActionConfig(maxNumSegments = 1, index = 0)
        val states = listOf(State("ForceMergeState", listOf(forceMergeActionConfig), listOf()))

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

        Thread.sleep(2000)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        val segmentCount = getSegmentCount(indexName)
        assertTrue("Segment count for [$indexName] was less than expected", segmentCount > 1)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Second execution: Index is set to read-only for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        assertEquals("true", getIndexBlocksWriteSetting(indexName))

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Fourth execution: Waits for force merge to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        val segmentCountAfterForceMerge = getSegmentCount(indexName)
        assertEquals("Segment count for [$indexName] after force merge is incorrect", 1, segmentCountAfterForceMerge)

        // Fifth execution: Set index back to read-write since it was not originally read-only
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        assertEquals("false", getIndexBlocksWriteSetting(indexName))
    }

    fun `test force merge on index already in read-only`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"

        // Create a Policy with one State that only preforms a force_merge Action
        val forceMergeActionConfig = ForceMergeActionConfig(maxNumSegments = 1, index = 0)
        val states = listOf(State("ForceMergeState", listOf(forceMergeActionConfig), listOf()))

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

        Thread.sleep(2000)

        // Add sample data to increase segment count, passing in a delay to ensure multiple segments get created
        insertSampleData(indexName, 3, 1000)

        val segmentCount = getSegmentCount(indexName)
        assertTrue("Segment count for [$indexName] was less than expected", segmentCount > 1)

        // Set index to read-only
        updateIndexSettings(
            indexName,
            Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
        )

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)

        // Will change the startTime each execution so that it triggers in 2 seconds
        // First execution: Policy is initialized
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Second execution: Index was already read-only and should remain so for force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        assertEquals("true", getIndexBlocksWriteSetting(indexName))

        // Third execution: Force merge operation is kicked off
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        // Fourth execution: Waits for force merge to complete, which will happen in this execution since index is small
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        val segmentCountAfterForceMerge = getSegmentCount(indexName)
        assertEquals("Segment count for [$indexName] after force merge is incorrect", 1, segmentCountAfterForceMerge)

        // Fifth execution: Index should remain in read-only since it was set before force_merge
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        Thread.sleep(3000)

        assertEquals("true", getIndexBlocksWriteSetting(indexName))
    }

    @Suppress("UNCHECKED_CAST")
    private fun getSegmentCount(index: String): Int {
        val statsResponse: Map<String, Any> = getStats(index)

        // Assert that shard count of stats response is 1 since the stats request being used is at the index level
        // (meaning the segment count in the response is aggregated) but segment count for force merge is going to be
        // validated per shard
        val shardsInfo = statsResponse["_shards"] as Map<String, Int>
        assertEquals("Shard count higher than expected", 1, shardsInfo["successful"])

        val indicesStats = statsResponse["indices"] as Map<String, Map<String, Map<String, Map<String, Any?>>>>
        return indicesStats[index]!!["primaries"]!!["segments"]!!["count"] as Int
    }

    /** Get stats for [index] */
    private fun getStats(index: String): Map<String, Any> {
        val response = client().makeRequest("GET", "/$index/_stats")

        assertEquals("Stats request failed", RestStatus.OK, response.restStatus())

        return response.asMap()
    }
}
