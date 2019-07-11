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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class ChangePolicy(
    val policyID: String,
    val state: String?
) : ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .field(ManagedIndexConfig.POLICY_ID_FIELD, policyID)
                .field(StateMetaData.STATE, state)
            .endObject()
        return builder
    }

    companion object {

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ChangePolicy {
            var policyID: String? = null
            var state: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.POLICY_ID_FIELD -> policyID = xcp.text()
                    StateMetaData.STATE -> state = xcp.textOrNull()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ChangePolicy.")
                }
            }

            return ChangePolicy(
                requireNotNull(policyID) { "ChangePolicy policy id is null" },
                state
            )
        }
    }
}
