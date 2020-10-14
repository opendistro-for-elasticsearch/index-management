package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Table
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class GetPoliciesRequest : ActionRequest {

    val table: Table
    val index: String

    constructor(
        table: Table,
        index: String
    ) : super() {
        this.table = table
        this.index = index
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        table = Table.readFrom(sin),
        index = sin.readString()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        table.writeTo(out)
        out.writeString(index)
    }
}
