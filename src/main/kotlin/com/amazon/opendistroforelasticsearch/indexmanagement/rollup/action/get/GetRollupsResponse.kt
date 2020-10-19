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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup.Companion.ROLLUP_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class GetRollupsResponse : ActionResponse, ToXContentObject {
    val rollups: List<Rollup>
    val totalRollups: Long
    val status: RestStatus

    constructor(
        rollups: List<Rollup>,
        totalRollups: Long,
        status: RestStatus
    ) : super() {
        this.rollups = rollups
        this.totalRollups = totalRollups
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        rollups = sin.readList(::Rollup),
        totalRollups = sin.readLong(),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeCollection(rollups)
        out.writeLong(totalRollups)
        out.writeEnum(status)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("total_rollups", totalRollups)
            .startArray("rollups")
            .apply {
                for (rollup in rollups) {
                    this.startObject()
                        .field(_ID, rollup.id)
                        .field(_SEQ_NO, rollup.seqNo)
                        .field(_PRIMARY_TERM, rollup.primaryTerm)
                        .field(ROLLUP_TYPE, rollup, XCONTENT_WITHOUT_TYPE)
                        .endObject()
                }
            }
            .endArray()
            .endObject()
    }
}
