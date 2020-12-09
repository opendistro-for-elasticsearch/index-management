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

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.optionalTimeField
import org.apache.logging.log4j.LogManager
import org.elasticsearch.cluster.AbstractDiffable
import org.elasticsearch.cluster.Diff
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
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

// ComposableIndexTemplate
// ManagedIndexMetaData
data class ISMTemplate(
    val indexPatterns: List<String>,
    val policyID: String,
    val priority: Int,
    val lastUpdatedTime: Instant
) : ToXContentObject, AbstractDiffable<ISMTemplate>() {

    init {
        require(indexPatterns.isNotEmpty()) { "at least give one index pattern" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(INDEX_PATTERN, indexPatterns)
            .field(POLICY_ID, policyID)
            .field(PRIORITY, priority)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(),
        sin.readString(),
        sin.readInt(),
        sin.readInstant()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indexPatterns)
        out.writeString(policyID)
        out.writeInt(priority)
        out.writeInstant(lastUpdatedTime)
    }

    companion object {
        const val INDEX_PATTERN = "index_patterns"
        const val POLICY_ID = "policy_id"
        const val PRIORITY = "priority"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"

        @Suppress("ComplexMethod")
        fun parse(xcp: XContentParser): ISMTemplate {
            val indexPatterns: MutableList<String> = mutableListOf()
            var policyID: String? = null
            var priority = 0
            var lastUpdatedTime: Instant? = null

            log.info("current token ${xcp.currentToken()}")
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                log.info("parse field name $fieldName")
                xcp.nextToken()

                when (fieldName) {
                    INDEX_PATTERN -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            indexPatterns.add(xcp.text())
                            log.info("field $indexPatterns")
                        }
                    }
                    POLICY_ID -> {
                        policyID = xcp.text()
                        log.info("field $policyID")
                    }
                    PRIORITY -> {
                        priority = if (xcp.currentToken() == Token.VALUE_NULL) 0 else xcp.intValue()
                    }
                    LAST_UPDATED_TIME_FIELD -> {
                        lastUpdatedTime = xcp.instant()
                        log.info("field last update time $lastUpdatedTime")
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ISMTemplate.")
                }
            }

            val result = ISMTemplate(
                indexPatterns,
                requireNotNull(policyID) { "policy id is null" },
                priority,
                lastUpdatedTime ?: Instant.now()
            )

            log.info("ism template parse result $result")
            // TODO check index pattern is empty or not
            return result
        }

        fun readISMTemplateDiffFrom(sin: StreamInput): Diff<ISMTemplate> = AbstractDiffable.readDiffFrom(::ISMTemplate, sin)
    }
}
