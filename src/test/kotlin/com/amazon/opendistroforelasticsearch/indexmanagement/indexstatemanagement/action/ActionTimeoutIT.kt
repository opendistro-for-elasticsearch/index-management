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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.hamcrest.collection.IsMapContaining
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
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                strict = false
            )
        }

        // the second execution we move into rollover action, we won't hit the timeout as this is the execution that sets the startTime
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertThat(
                "Should be attempting to rollover",
                getExplainManagedIndexMetaData(indexName).info,
                IsMapContaining.hasEntry("message", AttemptRolloverStep.getAttemptingMessage(indexName) as Any?)
            )
        }

        // the third execution we should hit the 1 second action timeout and fail
        updateManagedIndexConfigStartTime(managedIndexConfig)
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

    // https://github.com/opendistro-for-elasticsearch/index-management/issues/130
    fun `test action timeout doesn't bleed over into next action`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val testPolicy = """
        {"policy":{"description":"Default policy","default_state":"rolloverstate","states":[
        {"name":"rolloverstate","actions":[{"timeout": "5s","open":{}},{"timeout":"1s","rollover":{"min_doc_count":100}}],
        "transitions":[]}]}}
        """.trimIndent()

        createPolicyJson(testPolicy, policyID)

        createIndex(indexName, policyID, "some_alias")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        // Change the start time so the job will trigger in 2 seconds.
        // First execution. We need to initialize the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                strict = false
            )
        }

        // the second execution we move into open action, we won't hit the timeout as this is the execution that sets the startTime
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val expectedOpenInfoString = mapOf("message" to AttemptOpenStep.getSuccessMessage(indexName)).toString()
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedOpenInfoString == info.toString())),
                getExplainMap(indexName),
                strict = false
            )
        }

        // wait 5 seconds for the timeout from the first action to have passed
        Thread.sleep(5000L)

        // the third execution we move into rollover action, we should not hit the timeout yet because its the first execution of rollover
        // but there was a bug before where it would use the startTime from the previous actions metadata and immediately fail
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertThat(
                "Should be attempting to rollover",
                getExplainManagedIndexMetaData(indexName).info,
                IsMapContaining.hasEntry("message", AttemptRolloverStep.getAttemptingMessage(indexName) as Any?)
            )
        }
    }
}
