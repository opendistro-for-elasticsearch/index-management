/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import org.elasticsearch.test.ESTestCase

class GetPolicyRequestTests : ESTestCase() {

    fun `test get policy request`() {
        val policyID = "policyID"
        val version: Long = 123
        val fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        val req = GetPolicyRequest(policyID, version, fetchSrcContext)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(version, newReq.version)
        assertEquals(fetchSrcContext, newReq.fetchSrcContext)
    }
}
