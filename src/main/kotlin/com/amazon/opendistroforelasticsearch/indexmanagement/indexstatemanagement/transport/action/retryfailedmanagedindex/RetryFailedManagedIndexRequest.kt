package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException
import java.util.Collections

class RetryFailedManagedIndexRequest : ActionRequest {

    val indices: List<String>
    val startState: String?

    constructor(
        indices: List<String>,
        state: String?
    ) : super() {
        this.indices = indices
        this.startState = state
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        Collections.unmodifiableList(sin.readStringList()),
        sin.readOptionalString()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeOptionalString(startState)
    }
}
