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

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.buildStreamInputForTransforms
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class WriteableTests : ESTestCase() {

    fun `test transform metadata as stream`() {
        val transformMetadata = randomTransformMetadata()
        val out = BytesStreamOutput().also { transformMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedTransformMetadata = TransformMetadata(sin)
        assertEquals("Round tripping TransformMetadata stream doesn't work", transformMetadata, streamedTransformMetadata)
    }

    fun `test transform as stream`() {
        val transform = randomTransform()
        val out = BytesStreamOutput().also { transform.writeTo(it) }
        val streamedTransform = Transform(buildStreamInputForTransforms(out))
        assertEquals("Round tripping Transform stream doesn't work", transform, streamedTransform)
    }
}
