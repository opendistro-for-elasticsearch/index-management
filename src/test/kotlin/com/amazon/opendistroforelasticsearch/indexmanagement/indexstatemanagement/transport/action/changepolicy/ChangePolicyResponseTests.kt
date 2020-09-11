package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class ChangePolicyResponseTests : ESTestCase() {

    fun `test change policy response`() {
        val updated = 1
        val failedIndex = FailedIndex("index", "uuid", "reason")
        val failedIndices = mutableListOf(failedIndex)

        val res = ChangePolicyResponse(updated, failedIndices)
        Assert.assertNotNull(res)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = ChangePolicyResponse(sin)
        Assert.assertEquals(updated, newRes.updated)
        Assert.assertEquals(failedIndices, newRes.failedIndices)
    }
}