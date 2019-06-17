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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models.actions

import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.WITH_TYPE
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class DeleteActionConfig(
    val timeout: String?,
    val retry: ActionRetry? // TODO: Specify default ActionRetry for delete action
) : ToXContentObject {

    init {
        if (timeout != null) {
            try {
                TimeValue.parseTimeValue(timeout, "")
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("Must have a valid timeout for DeleteActionConfig")
            }
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(DELETE_ACTION_TYPE)
        if (timeout != null) builder.field(TIMEOUT_FIELD, timeout)
        if (retry != null) builder.field(RETRY_FIELD, retry)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val DELETE_ACTION_TYPE = "delete"

        const val TIMEOUT_FIELD = "timeout"
        const val RETRY_FIELD = "retry"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): DeleteActionConfig {
            var timeout: String? = null
            var retry: ActionRetry? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TIMEOUT_FIELD -> timeout = xcp.text()
                    RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in DeleteActionConfig.")
                }
            }

            return DeleteActionConfig(timeout, retry)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun parseWithType(xcp: XContentParser): DeleteActionConfig {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val deleteActionConfig = parse(xcp)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return deleteActionConfig
        }
    }
}
