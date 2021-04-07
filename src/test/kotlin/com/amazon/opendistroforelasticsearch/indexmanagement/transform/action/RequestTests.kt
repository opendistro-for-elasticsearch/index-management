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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.buildStreamInputForTransforms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.test.ESTestCase

class RequestTests : ESTestCase() {

    fun `test delete single transform request`() {
        val id = "some_id"
        val req = DeleteTransformsRequest(listOf(id))

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = DeleteTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals(listOf(id), streamedReq.ids)
    }

    fun `test delete multiple transform request`() {
        val ids = mutableListOf("some_id", "some_other_id")
        val req = DeleteTransformsRequest(ids)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = DeleteTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals(ids, streamedReq.ids)
    }

    fun `test index transform create request`() {
        val transform = randomTransform().copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val req = IndexTransformRequest(
                transform = transform,
                refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = IndexTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
        assertEquals(transform.seqNo, streamedReq.ifSeqNo())
        assertEquals(transform.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.CREATE, streamedReq.opType())
    }

    fun `test index transform update request`() {
        val transform = randomTransform().copy(seqNo = 1L, primaryTerm = 2L)
        val req = IndexTransformRequest(
                transform = transform,
                refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = IndexTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
        assertEquals(transform.seqNo, streamedReq.ifSeqNo())
        assertEquals(transform.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.INDEX, streamedReq.opType())
    }

    fun `test preview transform request`() {
        val transform = randomTransform()
        val req = PreviewTransformRequest(transform = transform)
        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = PreviewTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
    }

    fun `test get transform request`() {
        val id = "some_id"
        val srcContext = null
        val preference = "_local"
        val req = GetTransformRequest(id, srcContext, preference)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
        assertEquals(preference, streamedReq.preference)
    }

    fun `test head get transform request`() {
        val id = "some_id"
        val srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        val req = GetTransformRequest(id, srcContext)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
    }

    fun `test get transforms request default`() {
        val req = GetTransformsRequest()

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals("", streamedReq.searchString)
        assertEquals(0, streamedReq.from)
        assertEquals(20, streamedReq.size)
        assertEquals("${Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", streamedReq.sortField)
        assertEquals("asc", streamedReq.sortDirection)
    }

    fun `test get transforms request`() {
        val req = GetTransformsRequest("searching", 10, 50, "sorted", "desc")

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals("searching", streamedReq.searchString)
        assertEquals(10, streamedReq.from)
        assertEquals(50, streamedReq.size)
        assertEquals("sorted", streamedReq.sortField)
        assertEquals("desc", streamedReq.sortDirection)
    }

    fun `test empty ids delete transforms request`() {
        val req = DeleteTransformsRequest(listOf())
        val validated = req.validate()
        assertNotNull("Expected validate to produce Exception", validated)
        assertEquals("org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: List of ids to delete is empty;", validated.toString())
    }
}
