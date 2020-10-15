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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.IndexPriorityActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomAuthHeader
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomUser
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class IndexPolicyRequestTests : ESTestCase() {

    fun `test index policy request index priority action`() {
        val policyID = "policyID"
        val actionConfig = IndexPriorityActionConfig(50, 0)
        val states = listOf(State(name = "SetPriorityState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            user = randomUser()
        )
        val seqNo: Long = 123
        val primaryTerm: Long = 456
        val refreshPolicy = WriteRequest.RefreshPolicy.NONE
        val req = IndexPolicyRequest(policyID, policy, seqNo, primaryTerm, refreshPolicy, randomAuthHeader())

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(policy, newReq.policy)
        assertEquals(seqNo, newReq.seqNo)
        assertEquals(primaryTerm, newReq.primaryTerm)
        assertEquals(policy, newReq.policy)
    }
}
