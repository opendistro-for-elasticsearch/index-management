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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalUserField
import org.apache.logging.log4j.LogManager
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
import java.lang.IllegalArgumentException
import java.time.Instant

private val log = LogManager.getLogger(ISMTemplate::class.java)

data class ISMTemplate(
    val indexPatterns: List<String>,
    val priority: Int,
    val lastUpdatedTime: Instant,
    val user: User?
) : ToXContentObject, Writeable {

    init {
        require(priority >= 0) { "Requires priority to be >= 0" }
        require(indexPatterns.isNotEmpty()) { "Requires at least one index pattern" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(INDEX_PATTERN, indexPatterns)
            .field(PRIORITY, priority)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .optionalUserField(USER_FIELD, user)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(),
        sin.readInt(),
        sin.readInstant(),
        sin.readOptionalWriteable(::User)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexPatterns)
        out.writeInt(priority)
        out.writeInstant(lastUpdatedTime)
        out.writeOptionalWriteable(user)
    }

    companion object {
        const val ISM_TEMPLATE_TYPE = "ism_template"
        const val INDEX_PATTERN = "index_patterns"
        const val PRIORITY = "priority"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val USER_FIELD = "user"

        @Suppress("ComplexMethod")
        fun parse(xcp: XContentParser): ISMTemplate {
            val indexPatterns: MutableList<String> = mutableListOf()
            var priority = 0
            var lastUpdatedTime: Instant? = null
            var user: User? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    INDEX_PATTERN -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            indexPatterns.add(xcp.text())
                        }
                    }
                    PRIORITY -> priority = if (xcp.currentToken() == Token.VALUE_NULL) 0 else xcp.intValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    USER_FIELD -> user = if (xcp.currentToken() == Token.VALUE_NULL) null else User.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ISMTemplate.")
                }
            }

            return ISMTemplate(
                indexPatterns = indexPatterns,
                priority = priority,
                lastUpdatedTime = lastUpdatedTime ?: Instant.now(),
                user = user
            )
        }
    }
}
