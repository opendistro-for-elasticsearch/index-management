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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.time.Instant

class TransportStartRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<StartRollupRequest, AcknowledgedResponse>(
    StartRollupAction.NAME, transportService, actionFilters, ::StartRollupRequest
) {

    private val logger = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        // TODO: If we allow updated this even when it's already true it means it'll keep getting rescheduled further out
        //  might make more sense to just get first, compare if its already started or not and handle that
        val now = Instant.now().toEpochMilli()
        request.index(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            .doc(mapOf(Rollup.ROLLUP_TYPE to mapOf(Rollup.ENABLED_FIELD to true,
                Rollup.ENABLED_TIME_FIELD to now, Rollup.LAST_UPDATED_TIME_FIELD to now)))
        client.update(request, object : ActionListener<UpdateResponse> {
            override fun onResponse(response: UpdateResponse) {
                actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
            }

            override fun onFailure(t: Exception) {
                actionListener.onFailure(t)
            }
        })
    }
}
