package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.StateFilter
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class ChangePolicyRequestTests : ESTestCase() {

    fun `test change policy request`() {
        val indices = listOf("index1", "index2")
        val stateFilter = StateFilter("state1")
        val changePolicy = ChangePolicy("policyID", "state1", listOf(stateFilter), true)
        val req = ChangePolicyRequest(indices, changePolicy)
        Assert.assertNotNull(req)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ChangePolicyRequest(sin)
        Assert.assertEquals(indices, newReq.indices)
        Assert.assertEquals(changePolicy, newReq.changePolicy)
    }
}