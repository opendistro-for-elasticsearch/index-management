package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.test.ESTestCase
import org.junit.Assert
import org.junit.Ignore

class RefreshSearchAnalyzerResponseTests : ESTestCase() {

    @Ignore
    fun `test refresh response parsing`() {
        val unitTestShard0Failure = DefaultShardOperationFailedException("testIndex", 0, Throwable("dummyFailure"))
        val unitTestShard1Failure = DefaultShardOperationFailedException("testIndex", 1, Throwable("dummyFailure"))
        val results : MutableMap<String, List<String>> = mutableMapOf()

        val refreshResponse = RefreshSearchAnalyzerResponse(2, 0, 2,
                listOf(unitTestShard0Failure, unitTestShard1Failure), results)

        val refreshResponse2 = roundTripRequest(refreshResponse)
        Assert.assertEquals(refreshResponse2.successfulShards, refreshResponse.successfulShards)
        Assert.assertEquals(refreshResponse2.failedShards, refreshResponse.failedShards)
        Assert.assertEquals(refreshResponse2.totalShards, refreshResponse.totalShards)
    }

    @Throws(Exception::class)
    private fun roundTripRequest(response: RefreshSearchAnalyzerResponse): RefreshSearchAnalyzerResponse {
        BytesStreamOutput().use { out ->
            response.writeTo(out)
            out.bytes().streamInput().use { `in` -> return RefreshSearchAnalyzerResponse(`in`) }
        }
    }
}