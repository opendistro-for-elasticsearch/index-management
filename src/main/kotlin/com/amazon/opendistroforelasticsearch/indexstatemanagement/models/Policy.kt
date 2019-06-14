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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.WITH_TYPE
import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

data class Policy(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val name: String,
    val schemaVersion: Long,
    val lastUpdatedTime: Instant,
    // TODO: Implement DefaultNotification(destination, message)
    val defaultNotification: Map<String, Any>?,
    val defaultState: String,
    val states: List<State>
) : ToXContentObject {

    init {
        require(states.isNotEmpty()) { "Policy must contain at least one State" }
        requireNotNull(states.find { it.name == defaultState }) { "Policy must have a valid default state" }
    }

    fun toXContent(builder: XContentBuilder): XContentBuilder {
        return toXContent(builder, ToXContent.EMPTY_PARAMS)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(POLICY_TYPE)
        builder.field(NAME_FIELD, name)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(DEFAULT_NOTIFICATION_FIELD, defaultNotification)
            .field(DEFAULT_STATE_FIELD, defaultState)
            .field(STATES_FIELD, states.toTypedArray())
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val POLICY_TYPE = "policy"
        const val NAME_FIELD = "name"
        const val NO_ID = ""
        const val NO_VERSION = Versions.NOT_FOUND
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val DEFAULT_NOTIFICATION_FIELD = "default_notification"
        const val DEFAULT_STATE_FIELD = "default_state"
        const val STATES_FIELD = "states"

        @Suppress("ComplexMethod")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Policy {
            lateinit var name: String
            lateinit var defaultState: String
            // TODO Implement DefaultNotification(destination, message)
            var defaultNotification: Map<String, Any>? = null
            var lastUpdatedTime: Instant? = null
            var schemaVersion: Long = 1
            val states: MutableList<State> = mutableListOf()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    NAME_FIELD -> name = xcp.text()
                    // TODO: DefaultNotification.parse(xcp)
                    DEFAULT_NOTIFICATION_FIELD -> defaultNotification = null
                    DEFAULT_STATE_FIELD -> defaultState = xcp.text()
                    STATES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp::getTokenLocation)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            states.add(State.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Policy.")
                }
            }

            return Policy(
                id,
                seqNo,
                primaryTerm,
                requireNotNull(name) { "Policy name is null" },
                schemaVersion,
                lastUpdatedTime ?: Instant.now(),
                defaultNotification,
                requireNotNull(defaultState) { "Default state is null" },
                states.toList()
            )
        }

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parseWithType(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Policy {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val policy = parse(xcp, id, seqNo, primaryTerm)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return policy
        }
    }
}
