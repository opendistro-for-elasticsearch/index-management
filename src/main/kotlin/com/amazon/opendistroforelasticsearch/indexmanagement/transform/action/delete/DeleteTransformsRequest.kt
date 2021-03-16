package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class DeleteTransformsRequest(
    val ids: List<String>
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        ids = sin.readStringList()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (ids.isEmpty()) {
            validationException = addValidationError("List of ids to delete is empty", validationException)
        }

        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(ids.toString())
    }
}
