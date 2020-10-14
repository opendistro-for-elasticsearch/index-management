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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import com.amazon.opendistroforelasticsearch.indexmanagement.util._VERSION
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.RestStatus
import java.io.IOException

class IndexPolicyResponse : ActionResponse, ToXContentObject {

    val id: String
    val version: Long
    val primaryTerm: Long
    val seqNo: Long
    val policy: Policy
    val status: RestStatus

    constructor(
        id: String,
        version: Long,
        primaryTerm: Long,
        seqNo: Long,
        policy: Policy,
        status: RestStatus
    ) : super() {
        this.id = id
        this.version = version
        this.primaryTerm = primaryTerm
        this.seqNo = seqNo
        this.policy = policy
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        primaryTerm = sin.readLong(),
        seqNo = sin.readLong(),
        policy = Policy(sin),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(primaryTerm)
        out.writeLong(seqNo)
        policy.writeTo(out)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_PRIMARY_TERM, primaryTerm)
            .field(_SEQ_NO, seqNo)
            .field(Policy.POLICY_TYPE, policy)
            .endObject()
    }
}
