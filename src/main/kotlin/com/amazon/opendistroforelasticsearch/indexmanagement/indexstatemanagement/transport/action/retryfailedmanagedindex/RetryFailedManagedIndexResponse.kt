package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class RetryFailedManagedIndexResponse : ActionResponse, ToXContentObject {

    val updated: Int
    val failedIndices: MutableList<FailedIndex>

    constructor(
        updated: Int,
        failedIndices: MutableList<FailedIndex>
    ) : super() {
        this.updated = updated
        this.failedIndices = failedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readInt(),
        sin.readList(::FailedIndex)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeInt(updated)
        out.writeCollection(failedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(UPDATED_INDICES, updated)
        buildInvalidIndexResponse(builder, failedIndices)
        return builder.endObject()
    }
}
