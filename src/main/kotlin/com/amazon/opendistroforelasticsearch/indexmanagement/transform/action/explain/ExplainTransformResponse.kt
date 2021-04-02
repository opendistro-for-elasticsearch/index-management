package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.ExplainTransform
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExplainTransformResponse : ActionResponse, ToXContentObject {
    val idsToExplain: Map<String, ExplainTransform?>

    constructor(idsToExplain: Map<String, ExplainTransform?>) : super() {
        this.idsToExplain = idsToExplain
    }

    internal fun getIdsToExplain(): Map<String, ExplainTransform?> {
        return this.idsToExplain
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        idsToExplain = sin.let {
            val idsToExplain = mutableMapOf<String, ExplainTransform?>()
            val size = it.readVInt()
            for (i in 0 until size) {
                idsToExplain[it.readString()] = if (sin.readBoolean()) ExplainTransform(it) else null
            }
            idsToExplain.toMap()
        }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeVInt(idsToExplain.size)
        idsToExplain.entries.forEach { (id, metadata) ->
            out.writeString(id)
            out.writeBoolean(metadata != null)
            metadata?.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        idsToExplain.entries.forEach { (id, explain) ->
            builder.field(id, explain)
        }
        return builder.endObject()
    }
}
