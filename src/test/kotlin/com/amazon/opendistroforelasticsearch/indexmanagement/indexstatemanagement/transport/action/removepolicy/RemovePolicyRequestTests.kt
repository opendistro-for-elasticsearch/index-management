package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RemovePolicyRequestTests : ESTestCase() {

    fun `test remove policy request`() {
        val indices = listOf("index1", "index2")
        val req = RemovePolicyRequest(indices)
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RemovePolicyRequest(sin)
        Assert.assertEquals(indices, newReq.indices)
    }
}
