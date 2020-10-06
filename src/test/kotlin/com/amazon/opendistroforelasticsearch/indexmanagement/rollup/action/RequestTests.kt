/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class RequestTests : ESTestCase() {

    fun `test index rollup post request`() {
        val rollup = randomRollup()
        val req = IndexRollupRequest(
            rollup = rollup,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = IndexRollupRequest(sin)
        assertEquals(rollup.id, streamedReq.rollup.id)
        assertEquals(rollup, streamedReq.rollup)
        assertEquals(rollup.seqNo, streamedReq.ifSeqNo())
        assertEquals(rollup.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
    }

    //    fun `test index monitor post request`() {
    //
    //        val req = IndexMonitorRequest("1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.POST,
    //                randomMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder()))))
    //        assertNotNull(req)
    //
    //        val out = BytesStreamOutput()
    //        req.writeTo(out)
    //        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
    //        val newReq = IndexMonitorRequest(sin)
    //        assertEquals("1234", newReq.monitorId)
    //        assertEquals(1L, newReq.seqNo)
    //        assertEquals(2L, newReq.primaryTerm)
    //        assertEquals(RestRequest.Method.POST, newReq.method)
    //        assertNotNull(newReq.monitor)
    //    }
    //
    //    fun `test index monitor put request`() {
    //
    //        val req = IndexMonitorRequest("1234", 1L, 2L, WriteRequest.RefreshPolicy.IMMEDIATE, RestRequest.Method.PUT,
    //                randomMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder()))))
    //        assertNotNull(req)
    //
    //        val out = BytesStreamOutput()
    //        req.writeTo(out)
    //        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
    //        val newReq = IndexMonitorRequest(sin)
    //        assertEquals("1234", newReq.monitorId)
    //        assertEquals(1L, newReq.seqNo)
    //        assertEquals(2L, newReq.primaryTerm)
    //        assertEquals(RestRequest.Method.PUT, newReq.method)
    //        assertNotNull(newReq.monitor)
    //    }
}