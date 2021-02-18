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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import java.io.IOException

data class TransformStats(
    val pagesProcessed: Long,
    val documentsProcessed: Long,
    val documentsIndexed: Long,
    val indexTimeInMillis: Long,
    val searchTimeInMillis: Long
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        pagesProcessed = sin.readLong(),
        documentsProcessed = sin.readLong(),
        documentsIndexed = sin.readLong(),
        indexTimeInMillis = sin.readLong(),
        searchTimeInMillis = sin.readLong()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(PAGES_PROCESSED_FIELD, pagesProcessed)
            .field(DOCUMENTS_PROCESSED_FIELD, documentsProcessed)
            .field(DOCUMENTS_INDEXED_FIELD, documentsIndexed)
            .field(INDEX_TIME_IN_MILLIS_FIELD, indexTimeInMillis)
            .field(SEARCH_TIME_IN_MILLIS_FIELD, searchTimeInMillis)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeLong(pagesProcessed)
        out.writeLong(documentsProcessed)
        out.writeLong(documentsIndexed)
        out.writeLong(indexTimeInMillis)
        out.writeLong(searchTimeInMillis)
    }

    companion object {
        private const val PAGES_PROCESSED_FIELD = "pages_processed"
        private const val DOCUMENTS_PROCESSED_FIELD = "documents_processed"
        private const val DOCUMENTS_INDEXED_FIELD = "documents_indexed"
        private const val INDEX_TIME_IN_MILLIS_FIELD = "index_time_in_millis"
        private const val SEARCH_TIME_IN_MILLIS_FIELD = "search_time_in_millis"

        @Suppress("ComplexMethod, LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): TransformStats {
            var pagesProcessed: Long? = null
            var documentsProcessed: Long? = null
            var documentsIndexed: Long? = null
            var indexTimeInMillis: Long? = null
            var searchTimeInMillis: Long? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    PAGES_PROCESSED_FIELD -> pagesProcessed = xcp.longValue()
                    DOCUMENTS_PROCESSED_FIELD -> documentsProcessed = xcp.longValue()
                    DOCUMENTS_INDEXED_FIELD -> documentsIndexed = xcp.longValue()
                    INDEX_TIME_IN_MILLIS_FIELD -> indexTimeInMillis = xcp.longValue()
                    SEARCH_TIME_IN_MILLIS_FIELD -> searchTimeInMillis = xcp.longValue()
                }
            }

            return TransformStats(
                pagesProcessed = requireNotNull(pagesProcessed) { "Pages processed must not be null" },
                documentsProcessed = requireNotNull(documentsProcessed) { "Documents processed must not be null" },
                documentsIndexed = requireNotNull(documentsIndexed) { "Documents indexed must not be null" },
                indexTimeInMillis = requireNotNull(indexTimeInMillis) { "Index time in millis must not be null" },
                searchTimeInMillis = requireNotNull(searchTimeInMillis) { "Search time in millis must not be null" }
            )
        }
    }
}
