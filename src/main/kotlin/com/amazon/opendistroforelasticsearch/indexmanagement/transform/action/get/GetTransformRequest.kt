package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetTransformRequest : ActionRequest {
    var id: String
    val srcContext: FetchSourceContext?
    val preference: String?

    constructor(
        id: String,
        srcContext: FetchSourceContext? = null,
        preference: String? = null
    ) : super() {
        this.id = id
        this.srcContext = srcContext
        this.preference = preference
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
            id = sin.readString(),
            srcContext = if (sin.readBoolean()) FetchSourceContext(sin) else null,
            preference = sin.readOptionalString()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {}
}
