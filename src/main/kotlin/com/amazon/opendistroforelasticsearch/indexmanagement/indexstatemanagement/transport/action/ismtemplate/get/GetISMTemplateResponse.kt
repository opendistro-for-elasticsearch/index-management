package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException

// GetComposableIndexTemplateAction.Response
class GetISMTemplateResponse : ActionResponse, ToXContentObject {

    val ismTemplates: Map<String, ISMTemplate>

    constructor(ismTemplates: Map<String, ISMTemplate>) : super() {
        this.ismTemplates = ismTemplates
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        val size = sin.readVInt()
        ismTemplates = mutableMapOf()
        repeat(size) {
            ismTemplates.put(sin.readString(), ISMTemplate(sin))
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeVInt(ismTemplates.size)
        ismTemplates.forEach { (k, v) ->
            out.writeString(k)
            v.writeTo(out)
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startArray(ISM_TEMPLATES)
        ismTemplates.forEach { (k, v) ->
            builder.startObject().field(TEMPLATE_NAME, k).field(ISM_TEMPLATE, v).endObject()
        }
        return builder.endArray().endObject()
    }

    companion object {
        val ISM_TEMPLATES = "ism_templates"
        val TEMPLATE_NAME = "template_name"
        val ISM_TEMPLATE = "ism_template"
    }
}
