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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.RollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.script.ScriptService
import java.io.IOException

// TODO: Not sure what interface should be
class RollupActionConfig(
    val index: Int,
    val rollup: Rollup
) : ToXContentObject, ActionConfig(ActionType.ROLLUP, index) {

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = RollupAction(clusterService, client, rollup, managedIndexMetaData, this)

    override fun isFragment(): Boolean {
        TODO("Not yet implemented")
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): RollupActionConfig {
            // TODO: not sure if we let customer set the id or we choose the id
            val rollup = Rollup.parse(xcp)

            return RollupActionConfig(
                    index = index,
                    rollup = rollup
            )
        }
    }
}
