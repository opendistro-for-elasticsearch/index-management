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

@file:Suppress("TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi

import com.amazon.opendistroforelasticsearch.indexmanagement.util.NO_ID
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

fun XContentBuilder.optionalTimeField(name: String, instant: Instant?): XContentBuilder {
    if (instant == null) {
        return nullField(name)
    }
    return this.timeField(name, name, instant.toEpochMilli())
}

fun XContentParser.instant(): Instant? {
    return when {
        currentToken() == XContentParser.Token.VALUE_NULL -> null
        currentToken().isValue -> Instant.ofEpochMilli(longValue())
        else -> {
            XContentParserUtils.throwUnknownToken(currentToken(), tokenLocation)
            null // unreachable
        }
    }
}

/**
 * Extension function for ES 6.3 and above that duplicates the ES 6.2 XContentBuilder.string() method.
 */
fun XContentBuilder.string(): String = BytesReference.bytes(this).utf8ToString()

@JvmOverloads
@Throws(IOException::class)
fun <T> XContentParser.parseWithType(
    id: String = NO_ID,
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T
): T {
    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, nextToken(), this::getTokenLocation)
    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, nextToken(), this::getTokenLocation)
    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, nextToken(), this::getTokenLocation)
    val parsed = parse(this, id, seqNo, primaryTerm)
    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, this.nextToken(), this::getTokenLocation)
    return parsed
}
