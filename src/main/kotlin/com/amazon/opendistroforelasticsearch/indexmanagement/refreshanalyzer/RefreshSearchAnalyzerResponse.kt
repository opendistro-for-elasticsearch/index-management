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

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.DefaultShardOperationFailedException.readShardOperationFailed
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.action.RestActions
import java.io.IOException
import java.util.function.Function

class RefreshSearchAnalyzerResponse : BroadcastResponse {

    protected var logger = LogManager.getLogger(javaClass)

    private lateinit var shardResponses: MutableList<RefreshSearchAnalyzerShardResponse>
    private lateinit var shardFailures: MutableList<DefaultShardOperationFailedException>

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        val resultSize: Int = inp.readVInt()
        for (i in 0..resultSize) {
            shardResponses.add(RefreshSearchAnalyzerShardResponse(inp))
        }

        val failureSize: Int = inp.readVInt()
        for (i in 0..failureSize) {
            shardFailures.add(readShardOperationFailed(inp))
        }
    }

    constructor(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>,
        shardResponses: List<RefreshSearchAnalyzerShardResponse>
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.shardResponses = shardResponses.toMutableList()
        this.shardFailures = shardFailures.toMutableList()
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder? {
        builder.startObject()
        RestActions.buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, -1, failedShards, shardFailures.toTypedArray())
        builder.startArray("successful_refreshes")
        val successfulIndices = getSuccessfulRefreshDetails()
        for (index in successfulIndices.keys) {
            val reloadedAnalyzers = successfulIndices.get(index)!!
            builder.startObject().field("index", index).startArray("refreshed_analyzers")
            for (analyzer in reloadedAnalyzers) {
                builder.value(analyzer)
            }
            builder.endArray().endObject()
        }
        builder.endArray().endObject()
        return builder
    }

    // TODO: restrict it for testing
    fun getSuccessfulRefreshDetails(): MutableMap<String, List<String>> {
        var successfulRefreshDetails: MutableMap<String, List<String>> = HashMap()
        var failedIndices = mutableSetOf<String>()
        for (failure in shardFailures) {
            failedIndices.add(failure.index()!!)
        }
        for (response in shardResponses) {
            if (!failedIndices.contains(response.index)) {
                successfulRefreshDetails.putIfAbsent(response.index, response.reloadedAnalyzers)
            }
        }
        return successfulRefreshDetails
    }

    companion object {
        private val PARSER = ConstructingObjectParser<RefreshSearchAnalyzerResponse, Void>("_refresh_search_analyzers", true,
                Function { arg: Array<Any> ->
                    val response = arg[0] as RefreshSearchAnalyzerResponse
                    RefreshSearchAnalyzerResponse(response.totalShards, response.successfulShards, response.failedShards,
                            response.shardFailures, response.shardResponses)
                })
        init {
            declareBroadcastFields(PARSER)
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)

        out.writeVInt(shardResponses.size)
        for (response in shardResponses) {
            response.writeTo(out)
        }

        out.writeVInt(shardFailures.size)
        for (failure in shardFailures) {
            failure.writeTo(out)
        }
    }
}
