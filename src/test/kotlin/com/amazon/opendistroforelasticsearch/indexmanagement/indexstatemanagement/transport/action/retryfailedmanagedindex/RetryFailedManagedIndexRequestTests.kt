package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RetryFailedManagedIndexRequestTests : ESTestCase() {

    fun `test retry managed index request`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val req = RetryFailedManagedIndexRequest(indices, startState)
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RetryFailedManagedIndexRequest(sin)
        Assert.assertEquals(indices, newReq.indices)
        Assert.assertEquals(startState, newReq.startState)
    }
}
