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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric

import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

abstract class Metric(val type: Type) : ToXContentObject, Writeable {

    enum class Type(val type: String) {
        AVERAGE("avg"),
        SUM("sum"),
        MAX("max"),
        MIN("min"),
        VALUE_COUNT("value_count");

        override fun toString(): String {
            return type
        }
    }

    companion object {
        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Metric {
            var metric: Metric? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                metric = when (fieldName) {
                    Type.AVERAGE.type -> Average.parse(xcp)
                    Type.MAX.type -> Max.parse(xcp)
                    Type.MIN.type -> Min.parse(xcp)
                    Type.SUM.type -> Sum.parse(xcp)
                    Type.VALUE_COUNT.type -> ValueCount.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid metric type [$fieldName] found in metrics")
                }
            }

            return requireNotNull(metric) { "Metric is null" }
        }
    }
}
