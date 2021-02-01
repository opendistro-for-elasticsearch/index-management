/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class GetPoliciesResponseTests : ESTestCase() {

    fun `test get policies response`() {
        val policy = randomPolicy()
        val res = GetPoliciesResponse(listOf(policy), 1)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetPoliciesResponse(sin)
        assertEquals(1, newRes.totalPolicies)
        assertEquals(1, newRes.policies.size)
        assertEquals(policy, newRes.policies[0])
    }
}
