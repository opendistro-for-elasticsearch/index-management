package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException
import java.util.Collections

class RemovePolicyRequest : ActionRequest {

    val indices: List<String>

    constructor(
        indices: List<String>
    ) : super() {
        this.indices = indices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        Collections.unmodifiableList(sin.readStringList())
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
    }
}
