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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomExplainTransform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain.ExplainTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.buildStreamInputForTransforms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.test.ESTestCase.randomList

class ResponseTests : ESTestCase() {

    fun `test explain transform response`() {
        val idsToExplain = randomList(10) { randomAlphaOfLength(10) to randomExplainTransform() }.toMap()
        val res = ExplainTransformResponse(idsToExplain)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = ExplainTransformResponse(buildStreamInputForTransforms(out))

        assertEquals(idsToExplain, streamedRes.idsToExplain)
    }

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

    fun `test get transform response null`() {
        val res = GetTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, null)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(null, streamedRes.transform)
    }

    fun `test get transform response`() {
        val transform = randomTransform()
        val res = GetTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, transform)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(transform, streamedRes.transform)
    }

    fun `test get transforms response`() {
        val transforms = randomList(1, 15) { randomTransform() }

        val res = GetTransformsResponse(transforms, transforms.size, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformsResponse(buildStreamInputForTransforms(out))

        assertEquals(transforms.size, streamedRes.totalTransforms)
        assertEquals(transforms.size, streamedRes.transforms.size)
        assertEquals(RestStatus.OK, streamedRes.status)
        for (i in 0 until transforms.size) {
            assertEquals(transforms[i], streamedRes.transforms[i])
        }
    }
}
