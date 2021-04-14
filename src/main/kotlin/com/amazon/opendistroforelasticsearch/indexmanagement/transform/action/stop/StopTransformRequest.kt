package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.stop

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class StopTransformRequest : UpdateRequest {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin)

    constructor(id: String) {
        super.id(id)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (super.id().isEmpty()) {
            validationException = addValidationError("id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }
}
