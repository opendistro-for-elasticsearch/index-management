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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step.forcemerge

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.apache.logging.log4j.LogManager

class WaitForForceMergeStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("wait_for_force_merge", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var failed: Boolean = false
    private var stepCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    // TODO: Incorporate retries from config and consumed retries from metadata
    @Suppress("TooGenericExceptionCaught") // TODO see if we can refactor to catch GenericException in Runner.
    override suspend fun execute() {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
    }
}
