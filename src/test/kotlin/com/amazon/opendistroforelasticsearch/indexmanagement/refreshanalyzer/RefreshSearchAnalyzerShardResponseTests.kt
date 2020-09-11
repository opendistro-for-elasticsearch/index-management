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

package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.index.Index
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RefreshSearchAnalyzerShardResponseTests : ESTestCase() {

    fun `test shard refresh response parsing`() {
        val reloadedAnalyzers = listOf("analyzer1", "analyzer2")
        val refreshShardResponse = RefreshSearchAnalyzerShardResponse(ShardId(Index("testIndex", "qwerty"), 0), reloadedAnalyzers)

        val refreshShardResponse2 = roundTripRequest(refreshShardResponse)
        Assert.assertEquals(refreshShardResponse2.shardId, refreshShardResponse.shardId)
    }

    @Throws(Exception::class)
    private fun roundTripRequest(response: RefreshSearchAnalyzerShardResponse): RefreshSearchAnalyzerShardResponse {
        BytesStreamOutput().use { out ->
            response.writeTo(out)
            out.bytes().streamInput().use { si -> return RefreshSearchAnalyzerShardResponse(si) }
        }
    }
}