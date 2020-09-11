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

import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RefreshSearchAnalyzerResponseTests : ESTestCase() {

    fun `test get successful refresh details`() {
        val index1 = "index1"
        val index2 = "index2"
        val syn1 = "synonym1"
        val syn2 = "synonym2"
        val i1s0 = ShardId(index1, "abc", 0)
        val i1s1 = ShardId(index1, "abc", 1)
        val i2s0 = ShardId(index2, "xyz", 0)
        val i2s1 = ShardId(index2, "xyz", 1)

        var response_i1s0 = RefreshSearchAnalyzerShardResponse(i1s0, listOf(syn1, syn2))
        var response_i1s1 = RefreshSearchAnalyzerShardResponse(i1s1, listOf(syn1, syn2))
        var response_i2s0 = RefreshSearchAnalyzerShardResponse(i2s0, listOf(syn1))
        var response_i2s1 = RefreshSearchAnalyzerShardResponse(i2s1, listOf(syn1))
        var failure_i1s0 = DefaultShardOperationFailedException(index1, 0, Throwable("dummyCause"))
        var failure_i1s1 = DefaultShardOperationFailedException(index1, 1, Throwable("dummyCause"))
        var failure_i2s0 = DefaultShardOperationFailedException(index2, 0, Throwable("dummyCause"))
        var failure_i2s1 = DefaultShardOperationFailedException(index2, 1, Throwable("dummyCause"))

        // Case 1: All shards successful
        var aggregate_response = listOf(response_i1s0, response_i1s1, response_i2s0, response_i2s1)
        var aggregate_failures = listOf<DefaultShardOperationFailedException>()
        var refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 4, 0, aggregate_failures, aggregate_response)
        var successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.containsKey(index1))
        Assert.assertTrue(successfulIndices.containsKey(index2))

        // Case 2: All shards failed
        aggregate_response = listOf<RefreshSearchAnalyzerShardResponse>()
        aggregate_failures = listOf(failure_i1s0, failure_i1s1, failure_i2s0, failure_i2s1)
        refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 0, 4, aggregate_failures, aggregate_response)
        successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.isEmpty())

        // Case 3: Some shards of an index fail, while some others succeed
        aggregate_response = listOf(response_i1s1, response_i2s0, response_i2s1)
        aggregate_failures = listOf(failure_i1s0)
        refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 3, 1, aggregate_failures, aggregate_response)
        successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.containsKey(index2))
        Assert.assertFalse(successfulIndices.containsKey(index1))
    }
}