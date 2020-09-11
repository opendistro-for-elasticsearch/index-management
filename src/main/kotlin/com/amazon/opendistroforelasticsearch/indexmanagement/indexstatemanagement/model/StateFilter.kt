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

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class StateFilter(val state: String) : Writeable {

    override fun writeTo(out: StreamOutput) {
        out.writeString(state)
    }

    companion object {
        const val STATE_FIELD = "state"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): StateFilter {
            var state: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    STATE_FIELD -> state = xcp.text()
                }
            }

            return StateFilter(requireNotNull(state) { "Must include a state when using include filter" })
        }

        fun fromStreamInput(sin: StreamInput): StateFilter {
            val state: String? = sin.readString()

            return StateFilter(requireNotNull(state) { "Must include a state when using include filter" })
        }
    }
}
