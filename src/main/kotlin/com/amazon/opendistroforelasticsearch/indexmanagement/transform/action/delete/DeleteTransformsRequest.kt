package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class DeleteTransformsRequest(
    val ids: List<String>,
    val index: String = IndexManagementPlugin.INDEX_MANAGEMENT_INDEX
) : BulkRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        ids = sin.readStringList(),
        index = sin.readString()
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
        out.writeStringCollection(ids)
        out.writeString(index)
    }
}
