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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.toJsonString
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchModule
import org.elasticsearch.test.ESTestCase

class XContentTests : ESTestCase() {

    fun `test transform metadata parsing without type`() {
        val transformMetadata = randomTransformMetadata()
        val transformMetadataString = transformMetadata.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedTransformMetadata = TransformMetadata.parse(
            parser(transformMetadataString), transformMetadata.id, transformMetadata.seqNo, transformMetadata.primaryTerm)
        assertEquals("Round tripping Transform metadata without type doesn't work", transformMetadata, parsedTransformMetadata)
    }

    fun `test transform metadata parsing with type`() {
        val transformMetadata = randomTransformMetadata()
        val transformMetadataString = transformMetadata.toJsonString()
        val parser = parserWithType(transformMetadataString)
        val parsedTransformMetadata = parser.parseWithType(
            transformMetadata.id, transformMetadata.seqNo, transformMetadata.primaryTerm, TransformMetadata.Companion::parse)
        assertEquals("Round tripping Transform metadata with type doesn't work", transformMetadata, parsedTransformMetadata)
    }

    fun `test transform parsing without type`() {
        val transform = randomTransform()
        val transformString = transform.toJsonString(XCONTENT_WITHOUT_TYPE)
        val parsedTransform = Transform.parse(parser(transformString), transform.id, transform.seqNo, transform.primaryTerm)
        assertEquals("Round tripping Transform without type doesn't work", transform, parsedTransform)
    }

    fun `test transform parsing with type`() {
        val transform = randomTransform()
        val transformString = transform.toJsonString()
        val parser = parserWithType(transformString)
        val parsedTransform = parser.parseWithType(transform.id, transform.seqNo, transform.primaryTerm, Transform.Companion::parse)
        assertEquals("Round tripping Transform with type doesn't work", transform, parsedTransform)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }

    override fun xContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(SearchModule(Settings.EMPTY, false, emptyList()).namedXContents)
    }
}