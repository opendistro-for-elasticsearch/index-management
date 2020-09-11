package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RemovePolicyResponseTests : ESTestCase() {

    fun `test remove policy response`() {
        val updated = 1
        val failedIndex = FailedIndex("index", "uuid", "reason")
        val failedIndices = mutableListOf(failedIndex)

        val res = RemovePolicyResponse(updated, failedIndices)
        Assert.assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = RemovePolicyResponse(sin)
        Assert.assertEquals(updated, newRes.updated)
        Assert.assertEquals(failedIndices, newRes.failedIndices)
    }
}
