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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.util._ID
import com.amazon.opendistroforelasticsearch.indexmanagement.util._PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util._SEQ_NO
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class GetPoliciesResponse : ActionResponse, ToXContentObject {

    val policies: List<Policy>
    val totalPolicies: Int

    constructor(
        policies: List<Policy>,
        totalPolicies: Int
    ) : super() {
        this.policies = policies
        this.totalPolicies = totalPolicies
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policies = sin.readList(::Policy),
        totalPolicies = sin.readInt()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(policies)
        out.writeInt(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startArray("policies")
            .apply {
                for (policy in policies) {
                    this.startObject()
                        .field(_ID, policy.id)
                        .field(_SEQ_NO, policy.seqNo)
                        .field(_PRIMARY_TERM, policy.primaryTerm)
                        .field(Policy.POLICY_TYPE, policy, XCONTENT_WITHOUT_TYPE)
                        .endObject()
                }
            }
            .endArray()
            .field("total_policies", totalPolicies)
            .endObject()
    }
}
