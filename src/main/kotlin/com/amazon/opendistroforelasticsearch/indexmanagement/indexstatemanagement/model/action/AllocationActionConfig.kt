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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.AllocationAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.script.ScriptService
import java.io.IOException

data class AllocationActionConfig(
    val require: Map<String, String>,
    val include: Map<String, String>,
    val exclude: Map<String, String>,
    val waitFor: Boolean = false,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ALLOCATION, index) {

    init {
        require(require.isNotEmpty() || include.isNotEmpty() || exclude.isNotEmpty()) { "At least one allocation parameter need to be specified." }
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = AllocationAction(clusterService, client, managedIndexMetaData, this)

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.ALLOCATION.type)
        if (require.isNotEmpty()) builder.field(REQUIRE, require)
        if (include.isNotEmpty()) builder.field(INCLUDE, include)
        if (exclude.isNotEmpty()) builder.field(EXCLUDE, exclude)
        return builder.field(WAIT_FOR, waitFor)
            .endObject()
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        require = suppressWarning(sin.readMap()),
        include = suppressWarning(sin.readMap()),
        exclude = suppressWarning(sin.readMap()),
        waitFor = sin.readBoolean(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(require)
        out.writeMap(include)
        out.writeMap(exclude)
        out.writeBoolean(waitFor)
        out.writeInt(index)
    }

    companion object {
        const val REQUIRE = "require"
        const val INCLUDE = "include"
        const val EXCLUDE = "exclude"
        const val WAIT_FOR = "wait_for"

        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): MutableMap<String, String> {
            return map as MutableMap<String, String>
        }

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): AllocationActionConfig {
            val require: MutableMap<String, String> = mutableMapOf()
            val include: MutableMap<String, String> = mutableMapOf()
            val exclude: MutableMap<String, String> = mutableMapOf()
            var waitFor = false

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    REQUIRE -> assignObject(xcp, require)
                    INCLUDE -> assignObject(xcp, include)
                    EXCLUDE -> assignObject(xcp, exclude)
                    WAIT_FOR -> waitFor = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in AllocationActionConfig.")
                }
            }
            return AllocationActionConfig(require, include, exclude, waitFor, index)
        }

        private fun assignObject(xcp: XContentParser, objectMap: MutableMap<String, String>) {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                objectMap[fieldName] = xcp.text()
            }
        }
    }
}
