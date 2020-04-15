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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.SnapshotAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.script.ScriptService
import java.io.IOException

data class SnapshotActionConfig(
    val repository: String?,
    val snapshot: String?,
    val includeGlobalState: Boolean?,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.SNAPSHOT, index) {

    init {
        require(repository != null) { "SnapshotActionConfig repository must be specified" }
        require(snapshot != null) { "SnapshotActionConfig snapshot must be specified" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.SNAPSHOT.type)
        if (repository != null) builder.field(REPOSITORY_FIELD, repository)
        if (snapshot != null) builder.field(SNAPSHOT_FIELD, snapshot)
        if (includeGlobalState != null) builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = SnapshotAction(clusterService, client, managedIndexMetaData, this)

    companion object {
        const val REPOSITORY_FIELD = "repository"
        const val SNAPSHOT_FIELD = "snapshot"
        const val INCLUDE_GLOBAL_STATE = "include_global_state"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): SnapshotActionConfig {
            var repository: String? = null
            var snapshot: String? = null
            var includeGlobalState: Boolean? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    REPOSITORY_FIELD -> repository = xcp.text()
                    SNAPSHOT_FIELD -> snapshot = xcp.text()
                    INCLUDE_GLOBAL_STATE -> includeGlobalState = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in SnapshotActionConfig.")
                }
            }

            return SnapshotActionConfig(
                repository = repository,
                snapshot = snapshot,
                includeGlobalState = includeGlobalState,
                index = index
            )
        }
    }
}
