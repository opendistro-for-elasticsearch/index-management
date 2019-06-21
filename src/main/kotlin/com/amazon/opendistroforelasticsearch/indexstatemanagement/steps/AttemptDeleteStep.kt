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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.steps

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.actions.DeleteActionConfig
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AttemptDeleteStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: DeleteActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)

    // TODO: Incorporate retries from config and consumed retries from metadata
    // TODO: Needs to return execute status after finishing, i.e. succeeded, noop, failed, failed info to update metadata
    override suspend fun execute() {
        val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { delete(DeleteIndexRequest(managedIndexMetaData.index), it) }
        if (response.isAcknowledged) {
            logger.info("request is acknowledged")
        } else {
            logger.info("request is NOT acknowledged")
        }
    }

    companion object {
        const val name = "attempt_delete"
    }
}
