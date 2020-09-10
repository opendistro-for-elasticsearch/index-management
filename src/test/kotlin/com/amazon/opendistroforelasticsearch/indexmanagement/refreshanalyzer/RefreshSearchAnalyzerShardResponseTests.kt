package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.index.Index
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.test.ESTestCase
import org.junit.Assert


class RefreshSearchAnalyzerShardResponseTests : ESTestCase() {

    fun `test shard refresh response parsing`() {
        val reloadedAnalyzers = listOf("analyzer1", "analyzer2")
        val refreshShardResponse = RefreshSearchAnalyzerShardResponse(ShardId(Index("testIndex", "qwerty"), 0),
                "testIndex", reloadedAnalyzers)

        val refreshShardResponse2 = roundTripRequest(refreshShardResponse)
        Assert.assertEquals(refreshShardResponse2.indexName, refreshShardResponse.indexName)
        Assert.assertEquals(refreshShardResponse2.shardId, refreshShardResponse.shardId)
    }

    @Throws(Exception::class)
    private fun roundTripRequest(response: RefreshSearchAnalyzerShardResponse): RefreshSearchAnalyzerShardResponse {
        BytesStreamOutput().use { out ->
            response.writeTo(out)
            out.bytes().streamInput().use { `in` -> return RefreshSearchAnalyzerShardResponse(`in`) }
        }
    }
}