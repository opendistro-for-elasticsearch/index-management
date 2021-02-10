/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain.ExplainRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupsResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomExplainRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.ESTestCase

class ResponseTests : ESTestCase() {

    fun `test explain rollup response`() {
        val idsToExplain = randomList(10) { randomAlphaOfLength(10) to randomExplainRollup() }.toMap()
        val rolesMap = idsToExplain.map { it.key to null }.toMap()
        val res = ExplainRollupResponse(idsToExplain, rolesMap)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = ExplainRollupResponse(sin)
        assertEquals(idsToExplain, streamedRes.idsToExplain)
    }

    fun `test get rollup response null`() {
        val res = GetRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, null)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(null, streamedRes.rollup)
    }

    fun `test get rollup response`() {
        val rollup = randomRollup()
        val res = GetRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, rollup)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(rollup, streamedRes.rollup)
    }

    fun `test get rollups response`() {
        val rollups = randomList(1, 15) { randomRollup() }
        val res = GetRollupsResponse(rollups, rollups.size, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetRollupsResponse(sin)
        assertEquals(rollups.size, streamedRes.totalRollups)
        assertEquals(rollups.size, streamedRes.rollups.size)
        assertEquals(RestStatus.OK, streamedRes.status)
        for (i in 0 until rollups.size) {
            assertEquals(rollups[i], streamedRes.rollups[i])
        }
    }

    fun `test index rollup response`() {
        val rollup = randomRollup()
        val res = IndexRollupResponse("someid", 1L, 2L, 3L, RestStatus.OK, rollup)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = IndexRollupResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(rollup, streamedRes.rollup)
    }
}