package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException
import java.util.*

class GetPoliciesResponse : ActionResponse, ToXContentObject {

    val policies: List<Policy>
    val totalPolicies: Int?

    constructor(
        policies: List<Policy>,
        totalPolicies: Int?
    ) : super() {
        this.policies = policies
        this.totalPolicies = totalPolicies
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
            policies = Collections.unmodifiableList(sin.readList(::Policy)),
            totalPolicies = sin.readOptionalInt()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(policies)
        out.writeOptionalInt(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
                .field("policies", policies)
                .field("totalPolicies", totalPolicies)

        return builder.endObject()
    }
}
