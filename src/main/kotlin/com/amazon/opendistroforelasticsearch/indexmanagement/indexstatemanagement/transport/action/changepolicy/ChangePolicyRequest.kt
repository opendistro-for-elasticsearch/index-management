package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException
import java.util.Collections

class ChangePolicyRequest : ActionRequest {

    val indices: List<String>
    val changePolicy: ChangePolicy

    constructor(
        indices: List<String>,
        changePolicy: ChangePolicy
    ) : super() {
        this.indices = indices
        this.changePolicy = changePolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        Collections.unmodifiableList(sin.readStringList()),
        sin.readOptionalWriteable { ChangePolicy.fromStreamInput(it) } as ChangePolicy
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeOptionalWriteable(changePolicy)
    }
}
