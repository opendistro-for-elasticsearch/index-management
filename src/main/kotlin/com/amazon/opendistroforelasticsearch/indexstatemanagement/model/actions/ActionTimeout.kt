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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.actions

import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import java.io.IOException

data class ActionTimeout(val timeout: TimeValue) : ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.field(TIMEOUT_FIELD, timeout.stringRep)
    }

    companion object {
        const val TIMEOUT_FIELD = "timeout"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ActionTimeout {
            if (xcp.currentToken() == Token.VALUE_STRING) {
                return ActionTimeout(TimeValue.parseTimeValue(xcp.text(), TIMEOUT_FIELD))
            } else {
                throw IllegalArgumentException("Invalid token: [${xcp.currentToken()}] for ActionTimeout")
            }
        }
    }
}
