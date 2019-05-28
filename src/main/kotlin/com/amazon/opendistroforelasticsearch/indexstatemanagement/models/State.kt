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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models

import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class State(
    val id: String = NO_ID,
    val version: Long = NO_VERSION,
    val stateName: String,
    val actions: List<Map<String, Any>>, // TODO: Implement List<Action>
    val transitions: List<Map<String, Any>> // TODO: Implement List<Transition>
) : ToXContent {

    fun toXContent(builder: XContentBuilder): XContentBuilder =
        toXContent(builder, ToXContent.EMPTY_PARAMS)

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .field(NAME_FIELD, stateName)
                .field(ACTIONS_FIELD, actions.toTypedArray())
                .field(TRANSITIONS_FIELD, transitions.toTypedArray())
            .endObject()
        return builder
    }

    companion object {
        const val NO_ID = ""
        const val NO_VERSION = Versions.NOT_FOUND
        const val NAME_FIELD = "state"
        const val ACTIONS_FIELD = "actions"
        const val TRANSITIONS_FIELD = "transitions"

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): State {
            lateinit var name: String
            val actions: MutableList<Map<String, Any>> = mutableListOf() // TODO: Implement List<Action>
            val transitions: MutableList<Map<String, Any>> = mutableListOf() // TODO: Implement List<Transition>

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    ACTIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            // actions.add(Action.parse(xcp)) // TODO: Implement Action.parse()
                        }
                    }
                    TRANSITIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            // transitions.add(Transition.parse(xcp)) // TODO: Implement Transition.parse()
                        }
                    }
                }
            }

            return State(
                    id,
                    version,
                    stateName = requireNotNull(name) { "State name is null" },
                    actions = requireNotNull(actions) { "State actions are null" },
                    transitions = requireNotNull(transitions) { "State transitions are null" }
            )
        }
    }
}