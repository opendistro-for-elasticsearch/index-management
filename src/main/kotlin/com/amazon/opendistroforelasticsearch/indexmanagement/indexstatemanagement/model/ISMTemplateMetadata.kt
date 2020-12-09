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
import org.elasticsearch.cluster.AbstractNamedDiffable
import org.elasticsearch.cluster.Diff
import org.elasticsearch.cluster.DiffableUtils
import org.elasticsearch.cluster.NamedDiff
import org.elasticsearch.cluster.metadata.ComponentTemplate
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg
import org.elasticsearch.common.xcontent.ContextParser
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.util.*
import java.util.stream.Stream

private val log = LogManager.getLogger(ISMTemplateMetadata::class.java)

/**
 * <template_name>: ISMTemplate
 */
// ComponentTemplateMetadata
// EnrichMetadata
class ISMTemplateMetadata(val ismTemplates: Map<String, ISMTemplate>): Metadata.Custom {

    constructor(sin: StreamInput) : this(
        sin.readMap(StreamInput::readString, ::ISMTemplate)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeMap(ismTemplates, StreamOutput::writeString) { stream, `val` -> `val`.writeTo(stream) }
    }

    override fun diff(before: Metadata.Custom): Diff<Metadata.Custom> {
        return ISMTemplateMetadataDiff((before as ISMTemplateMetadata), this)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        log.info("ism template metadata toXContent: $ismTemplates")
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

    class ISMTemplateMetadataDiff: NamedDiff<Metadata.Custom> {

        val ismTemplateDiff: Diff<Map<String, ISMTemplate>>

        constructor(before: ISMTemplateMetadata, after: ISMTemplateMetadata) {
            this.ismTemplateDiff = DiffableUtils.diff(before.ismTemplates, after.ismTemplates, DiffableUtils.getStringKeySerializer())
        }

        constructor(sin: StreamInput) {
            this.ismTemplateDiff = DiffableUtils.readJdkMapDiff(sin, DiffableUtils.getStringKeySerializer(),
            ::ISMTemplate, ISMTemplate.Companion::readISMTemplateDiffFrom)
        }

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

        val PARSER = ConstructingObjectParser<ISMTemplateMetadata, Void>(TYPE, false
        ) { a -> ISMTemplateMetadata(a[0] as Map<String, ISMTemplate>) }

        init {
            PARSER.declareObject(constructorArg(), ContextParser<Void, Map<String, ISMTemplate>> { p, _ ->
                val templates = mutableMapOf<String, ISMTemplate>()
                while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                    val name = p.currentName()
                    templates[name] = ISMTemplate.parse(p)
                }
                templates
            }, ISM_TEMPLATE)
        }

        fun fromStreamInput(sin: StreamInput) = ISMTemplateMetadata(sin.readMap(StreamInput::readString, ::ISMTemplate))

        // fun parse(xcp: XContentParser): ISMTemplateMetadata = PARSER.parse(xcp, null)
        fun parse(xcp: XContentParser): ISMTemplateMetadata {
            val ismTemplates = mutableMapOf<String, ISMTemplate>()
            log.info("ism template metadata parse, first token ${xcp.currentToken()}")
            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                log.info("current field name $fieldName")
                ismTemplates[fieldName] = ISMTemplate.parse(xcp)
            }
            return ISMTemplateMetadata(ismTemplates)
        }

        fun readDiffFrom(sin: StreamInput): NamedDiff<Metadata.Custom> = ISMTemplateMetadataDiff(sin)
    }
}
