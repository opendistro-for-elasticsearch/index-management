package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.buildStreamInputForTransforms
import org.elasticsearch.test.ESTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.index.seqno.SequenceNumbers

class RequestTests : ESTestCase() {

    fun `test index transform post request`() {
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

    fun `test index transform put request`() {
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
}
