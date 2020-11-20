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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

/**
 * The ChangePolicy data class is used for the eventual changing of policies in a managed index.
 * Whether it be completely different policies, or the same policy, but a new version. If a ChangePolicy
 * exists on a [ManagedIndexConfig] then when we are in the transition part of a state we will always check
 * if a ChangePolicy exists and attempt to change the policy. If the change happens, the ChangePolicy on
 * [ManagedIndexConfig] will be reset to null.
 *
 * The list of [StateFilter] are only used in the ChangePolicy API call where we will use them to filter out
 * the [ManagedIndexConfig]s to apply the ChangePolicy to.
 */
data class ChangePolicy(
    val policyID: String,
    val state: String?,
    val include: List<StateFilter>,
    val isSafe: Boolean
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        state = sin.readOptionalString(),
        include = sin.readList(::StateFilter),
        isSafe = sin.readBoolean()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .field(ManagedIndexConfig.POLICY_ID_FIELD, policyID)
                .field(StateMetaData.STATE, state)
                .field(IS_SAFE_FIELD, isSafe)
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
        out.writeOptionalString(state)
        out.writeList(include)
        out.writeBoolean(isSafe)
    }

    companion object {
        const val POLICY_ID_FIELD = "policy_id"
        const val STATE_FIELD = "state"
        const val INCLUDE_FIELD = "include"
        const val IS_SAFE_FIELD = "is_safe"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ChangePolicy {
            var policyID: String? = null
            var state: String? = null
            var isSafe: Boolean = false
            val include = mutableListOf<StateFilter>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_ID_FIELD -> policyID = xcp.text()
                    STATE_FIELD -> state = xcp.textOrNull()
                    INCLUDE_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            include.add(StateFilter.parse(xcp))
                        }
                    }
                    IS_SAFE_FIELD -> isSafe = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ChangePolicy.")
                }
            }

            return ChangePolicy(
                requireNotNull(policyID) { "ChangePolicy policy id is null" },
                state,
                include.toList(),
                isSafe
            )
        }
    }
}
