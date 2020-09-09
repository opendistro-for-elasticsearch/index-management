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
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.action.RestActions
import java.io.IOException
import java.util.function.Function

class RefreshSynonymAnalyzerResponse : BroadcastResponse {

    private var results: MutableMap<String, List<String>> = HashMap()
    private var shardFailures: MutableList<FailedShardDetails> = mutableListOf()
    private var temp: List<DefaultShardOperationFailedException> = mutableListOf()

    protected var logger = LogManager.getLogger(javaClass)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        val resultSize: Int = inp.readVInt()
        for (i in 0..resultSize) {
            results.put(inp.readString(), inp.readStringArray().toList())
        }

        val failureSize: Int = inp.readVInt()
        for (i in 0..failureSize) {
            shardFailures.add(FailedShardDetails(inp.readString(), inp.readInt(), inp.readString()))
        }
    }

    constructor(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>,
        results: MutableMap<String, List<String>>
    ) : super(
            totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.results = results
        this.temp = shardFailures
        for (failure in shardFailures) {
            this.shardFailures.add(FailedShardDetails(failure.index()!!, failure.shardId(), failure.reason()))
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder? {
        builder.startObject()
        RestActions.buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, -1, failedShards, null)

        builder.startArray("_successful")
        for (index in results.keys) {
            builder.startObject()
            val reloadedAnalyzers: List<String> = results.get(index)!!
            builder.field("index", index)
            builder.startArray("refreshed_analyzers")
            for (analyzer in reloadedAnalyzers) {
                builder.value(analyzer)
            }
            builder.endArray()
            builder.endObject()
        }
        builder.endArray()

        builder.startArray("_failed")
        for (failure in shardFailures) {
            builder.startObject()
            builder.value(failure.index)
            builder.value(failure.shardId)
            builder.value(failure.failureReason)
            builder.endObject()
        }
        builder.endArray()

        builder.endObject()
        return builder
    }

    companion object {
        private val PARSER = ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void>("refresh_synonym_analyzer", true,
                Function { arg: Array<Any> ->
                    val response = arg[0] as RefreshSynonymAnalyzerResponse
                    RefreshSynonymAnalyzerResponse(response.totalShards, response.successfulShards, response.failedShards,
                            response.temp, response.results)
                })

        init {
            declareBroadcastFields(PARSER)
        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)

        out.writeVInt(results.size)
        for ((key, value) in results.entries) {
            out.writeString(key)
            out.writeStringArray(value.toTypedArray())
        }

        out.writeVInt(shardFailures.size)
        for (failure in shardFailures) {
            out.writeString(failure.index)
            out.writeInt(failure.shardId)
            out.writeString(failure.failureReason)
        }
    }

    class FailedShardDetails(index: String, shardId: Int, failureReason: String) {
        val index = index
        val shardId = shardId
        val failureReason = failureReason
    }
}
