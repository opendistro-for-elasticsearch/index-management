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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.runner

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit

class ManagedIndexRunnerIT : IndexStateManagementRestTestCase() {

    fun `test version conflict fails job`() {
        val indexName = "version_conflict_index"
        val policyID = "version_conflict_policy"
        val actionConfig = OpenActionConfig(0)
        val states = listOf(State("OpenState", listOf(actionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$indexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy on managed index
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(ManagedIndexMetaData.POLICY_ID to policyID::equals)),
                getExplainMap(indexName),
                strict = false
            )
        }

        // change policy seqNo on managed index
        updateManagedIndexConfigPolicySeqNo(managedIndexConfig.copy(policySeqNo = 17))

        // start execution to see if it moves to failed because of version conflict
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        val expectedInfoString = mapOf("message" to "There is a version conflict between your previous execution and your managed index").toString()
        waitFor {
            assertPredicatesOnMetaData(
                listOf(indexName to listOf(
                    PolicyRetryInfoMetaData.RETRY_INFO to fun(retryInfoMetaDataMap: Any?): Boolean =
                        assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                    ManagedIndexMetaData.INFO to fun(info: Any?): Boolean = expectedInfoString == info.toString()
                )),
                getExplainMap(indexName),
                strict = false
            )
        }
    }
}
