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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
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

data class State(
    val name: String,
    val actions: List<ActionConfig>,
    val transitions: List<Transition>
) : ToXContentObject, Writeable {

    init {
        require(name.isNotBlank()) { "State must contain a valid name" }
        var hasDelete = false
        actions.forEach { actionConfig ->
            // dont allow actions after delete as they will never happen
            require(!hasDelete) { "State=$name must not contain an action after a delete action" }
            hasDelete = actionConfig.type == ActionConfig.ActionType.DELETE
        }

        // dont allow transitions if state contains delete
        if (hasDelete) require(transitions.isEmpty()) { "State=$name cannot contain transitions if using delete action" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .field(NAME_FIELD, name)
                .field(ACTIONS_FIELD, actions.toTypedArray())
                .field(TRANSITIONS_FIELD, transitions.toTypedArray())
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readList { ActionConfig.fromStreamInput(it) },
        sin.readList(::Transition)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeList(actions)
        out.writeList(transitions)
    }

    companion object {
        const val NAME_FIELD = "name"
        const val ACTIONS_FIELD = "actions"
        const val TRANSITIONS_FIELD = "transitions"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): State {
            var name: String? = null
            val actions: MutableList<ActionConfig> = mutableListOf()
            val transitions: MutableList<Transition> = mutableListOf()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    ACTIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            actions.add(ActionConfig.parse(xcp, actions.size))
                        }
                    }
                    TRANSITIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            transitions.add(Transition.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in State.")
                }
            }

            return State(
                name = requireNotNull(name) { "State name is null" },
                actions = actions.toList(),
                transitions = transitions.toList()
            )
        }
    }
}
