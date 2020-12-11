/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import org.apache.logging.log4j.LogManager
import org.elasticsearch.Version
import org.elasticsearch.cluster.Diff
import org.elasticsearch.cluster.DiffableUtils
import org.elasticsearch.cluster.NamedDiff
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import java.io.IOException
import java.util.EnumSet

private val log = LogManager.getLogger(ISMTemplateMetadata::class.java)

/**
 * <template_name>: ISMTemplate
 */
// ComponentTemplateMetadata
// EnrichMetadata
class ISMTemplateMetadata(val ismTemplates: Map<String, ISMTemplate>) : Metadata.Custom {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readMap(StreamInput::readString, ::ISMTemplate)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeMap(ismTemplates, StreamOutput::writeString) { stream, `val` -> `val`.writeTo(stream) }
    }

    override fun diff(before: Metadata.Custom): Diff<Metadata.Custom> {
        return ISMTemplateMetadataDiff((before as ISMTemplateMetadata), this)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject(ISM_TEMPLATE.preferredName)
        ismTemplates.forEach { (k, v) ->
            builder.field(k, v)
        }
        builder.endObject()
        return builder
    }

    override fun getWriteableName(): String = TYPE

    override fun getMinimalSupportedVersion(): Version = Version.V_7_7_0

    override fun context(): EnumSet<Metadata.XContentContext> = Metadata.ALL_CONTEXTS

    class ISMTemplateMetadataDiff : NamedDiff<Metadata.Custom> {

        val ismTemplateDiff: Diff<Map<String, ISMTemplate>>

        constructor(before: ISMTemplateMetadata, after: ISMTemplateMetadata) {
            this.ismTemplateDiff = DiffableUtils.diff(before.ismTemplates, after.ismTemplates, DiffableUtils.getStringKeySerializer())
        }

        @Throws(IOException::class)
        constructor(sin: StreamInput) {
            this.ismTemplateDiff = DiffableUtils.readJdkMapDiff(sin, DiffableUtils.getStringKeySerializer(),
                    ::ISMTemplate, ISMTemplate.Companion::readISMTemplateDiffFrom)
        }

        @Throws(IOException::class)
        override fun writeTo(out: StreamOutput) {
            ismTemplateDiff.writeTo(out)
        }

        override fun getWriteableName(): String = TYPE

        override fun apply(part: Metadata.Custom): Metadata.Custom {
            return ISMTemplateMetadata(ismTemplateDiff.apply((part as ISMTemplateMetadata).ismTemplates))
        }
    }

    companion object {
        val TYPE = "ism_template"
        val ISM_TEMPLATE = ParseField("ism_template")

        fun readDiffFrom(sin: StreamInput): NamedDiff<Metadata.Custom> = ISMTemplateMetadataDiff(sin)
    }
}
