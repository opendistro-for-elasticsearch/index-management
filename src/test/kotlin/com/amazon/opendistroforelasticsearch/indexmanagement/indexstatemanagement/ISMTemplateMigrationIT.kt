/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import java.time.Instant

class ISMTemplateMigrationIT : IndexStateManagementRestTestCase() {
    fun `test v1 index templates migration`() {
        val policyID1 = "p1"
        val policyID2 = "p2"
        createPolicy(randomPolicy(), policyID1)
        createPolicy(randomPolicy(), policyID2)
        createV1Template("t1", "a*", policyID1)
        createV1Template("t2", "ab*", policyID2)
        enableISMTemplateMigration()

        // cat templates, check t1 t2 order
        logger.info("cat index templates")
        logger.info(catIndexTemplates())

        val order = catIndexTemplates().map { it ->
            val row = it as Map<String, String>
            row["name"]
        }

        // t1, t2
        if (order == listOf("t1", "t2")) {
            waitFor(Instant.ofEpochSecond(80)) {
                assertEquals(getPolicy(policyID1).ismTemplates?.indexPatterns.toString(), "[a*]")
                assertEquals(getPolicy(policyID1).ismTemplates?.priority, 0)
            }
        }

        // t2, t1
        if (order == listOf("t2", "t1")) {
            waitFor(Instant.ofEpochSecond(80)) {
                assertEquals(getPolicy(policyID1).ismTemplates?.indexPatterns.toString(), "[a*]")
                assertEquals(getPolicy(policyID1).ismTemplates?.priority, 0)
                assertEquals(getPolicy(policyID1).ismTemplates?.indexPatterns.toString(), "[ab*]")
                assertEquals(getPolicy(policyID1).ismTemplates?.priority, 1)
            }
        }

        // 1s interval to let the ism_template becomes searchable so that coordinator
        // can pick it up
        Thread.sleep(1_000)
        deleteV1Template("t1")

        if (order == listOf("t1", "t2")) {
            val indexName = "ab_index"
            createIndex(indexName, policyID = null)
            waitFor {
                assertPredicatesOnMetaData(
                    listOf(
                        indexName to listOf(
                            ManagedIndexSettings.POLICY_ID.key to policyID1::equals
                        )
                    ), getExplainMap(indexName), false
                )
            }
        }

        if (order == listOf("t2", "t1")) {
            val indexName = "ab_index"
            createIndex(indexName, policyID = null)
            waitFor {
                assertPredicatesOnMetaData(
                    listOf(
                        indexName to listOf(
                            ManagedIndexSettings.POLICY_ID.key to policyID2::equals
                        )
                    ), getExplainMap(indexName), false
                )
            }
        }
    }

    private fun enableISMTemplateMigration() {
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_ENABLED.key, "-1")
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_ENABLED.key, "0")
    }
}