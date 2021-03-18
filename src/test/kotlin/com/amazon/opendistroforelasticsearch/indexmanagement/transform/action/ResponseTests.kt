/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview.PreviewTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.buildStreamInputForTransforms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase

class ResponseTests : ESTestCase() {

    fun `test index transform response`() {
        val transform = randomTransform()
        val res = IndexTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, transform)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = IndexTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(transform, streamedRes.transform)
    }

    fun `test preview transform response`() {
        val documents = listOf(
            mapOf("a" to mapOf<String, Any>("90.0" to 100), "b" to "id1", "c" to 100),
            mapOf("a" to mapOf<String, Any>("90.0" to 50), "b" to "id2", "c" to 20)
        )
        val res = PreviewTransformResponse(documents, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = PreviewTransformResponse(sin)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(documents, streamedRes.documents)
    }
}
