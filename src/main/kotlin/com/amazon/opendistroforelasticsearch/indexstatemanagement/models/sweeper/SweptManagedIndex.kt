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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models.sweeper

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.XContentType
import java.io.IOException

/**
 * Data class to hold partial [ManagedIndex] data.
 *
 * This data class is used in the [com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexSweeper]
 * to hold partial data when reading in the [ManagedIndex] document from the index.
 */
data class SweptManagedIndex(
    val index: String,
    val seqNo: Long,
    val primaryTerm: Long,
    val uuid: String,
    val policyName: String,
    val changePolicy: ChangePolicy?
) {

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, seqNo: Long, primaryTerm: Long): SweptManagedIndex {
            lateinit var index: String
            lateinit var uuid: String
            lateinit var policyName: String
            var changePolicy: ChangePolicy? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndex.INDEX_FIELD -> index = xcp.text()
                    ManagedIndex.INDEX_UUID_FIELD -> uuid = xcp.text()
                    ManagedIndex.POLICY_NAME_FIELD -> policyName = xcp.text()
                    ManagedIndex.CHANGE_POLICY_FIELD -> {
                        changePolicy = if (xcp.currentToken() == Token.VALUE_NULL) null else ChangePolicy.parse(xcp)
                    }
                }
            }

            return SweptManagedIndex(
                    requireNotNull(index) { "SweptManagedIndex index is null" },
                    seqNo,
                    primaryTerm,
                    requireNotNull(uuid) { "SweptManagedIndex uuid is null" },
                    requireNotNull(policyName) { "SweptManagedIndex policy name is null" },
                    changePolicy
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun parseWithType(xcp: XContentParser, seqNo: Long, primaryTerm: Long): SweptManagedIndex {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val sweptManagedIndex = parse(xcp, seqNo, primaryTerm)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return sweptManagedIndex
        }

        fun parser(bytesReference: BytesReference): XContentParser {
            return XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON)
        }
    }
}
