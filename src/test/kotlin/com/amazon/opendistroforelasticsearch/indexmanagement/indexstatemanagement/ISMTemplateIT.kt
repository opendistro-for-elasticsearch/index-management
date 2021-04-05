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

class ISMTemplateIT : IndexStateManagementRestTestCase() {
    fun `test v1 index templates migration`() {
        createV1Template("t1", "a*", "p1")
        createV1Template("t2", "ab*", "p2")
        createPolicy(randomPolicy(), "p1")
        createPolicy(randomPolicy(), "p2")
        enableISMTemplateMigration()

        // wait for p1, p2 ism_template field not null
        waitFor(Instant.ofEpochSecond(80)) {
            assertEquals(getPolicy("p1").ismTemplate?.indexPatterns.toString(), "[a*, -ab*]")
            assertEquals(getPolicy("p2").ismTemplate?.indexPatterns.toString(), "[ab*]")
        }

        deleteV1Template("t1")
        val indexName = "ab_index"
        createIndex(indexName, policyID = null)
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to listOf(
                        ManagedIndexSettings.POLICY_ID.key to "p2"::equals
                    )
                ), getExplainMap(indexName), false)
        }
    }

    private fun enableISMTemplateMigration() {
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_ENABLED.key, "-1")
        updateClusterSetting(ManagedIndexSettings.TEMPLATE_MIGRATION_ENABLED.key, "0")
    }
}