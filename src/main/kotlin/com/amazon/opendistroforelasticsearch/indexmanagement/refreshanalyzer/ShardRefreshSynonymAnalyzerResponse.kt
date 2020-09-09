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

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class ShardRefreshSynonymAnalyzerResponse : BroadcastShardResponse {
    var indexName: String
    var reloadedAnalyzers: List<String>

    constructor(`in`: StreamInput) : super(`in`) {
        indexName = `in`.readString()
        reloadedAnalyzers = `in`.readStringList()
    }

    constructor(shardId: ShardId?, indexName: String, reloadedAnalyzers: List<String>) : super(shardId) {
        this.indexName = indexName
        this.reloadedAnalyzers = reloadedAnalyzers
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalString(indexName)
        out.writeStringArray(reloadedAnalyzers.toTypedArray())
    }
}
