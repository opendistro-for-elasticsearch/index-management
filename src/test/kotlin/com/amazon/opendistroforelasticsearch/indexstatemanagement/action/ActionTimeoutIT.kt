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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import java.time.Instant
import java.util.Locale

class ActionTimeoutIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test failed action`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"rolloverstate","states":[
        {"name":"rolloverstate","actions":[{"timeout":"1s","rollover":{"min_doc_count":100}}],
        "transitions":[]}]}}
        """.trimIndent()

        createPolicyJson(testPolicy, policyID)

        createIndex(indexName, policyID, "some_alias")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        // First execution. We need to initialize the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                strict = false
            )
        }

        // the second execution we move into rollover action, we won't hit the timeout as this is the execution that sets the startTime
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        val expectedInfoString = mapOf("message" to "Attempting to rollover").toString()
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString())),
                getExplainMap(indexName),
                strict = false
            )
        }

        // the third execution we should hit the 1 second action timeout and fail
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ActionMetaData.ACTION to fun(actionMetaDataMap: Any?): Boolean =
                    assertActionEquals(ActionMetaData(name = ActionConfig.ActionType.ROLLOVER.type, startTime = Instant.now().toEpochMilli(), index = 0,
                        failed = true, consumedRetries = 0, lastRetryTime = null, actionProperties = null), actionMetaDataMap))),
                getExplainMap(indexName),
                strict = false
            )
        }
    }
}
