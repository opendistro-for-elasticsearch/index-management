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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ANALYZER_BASE_URI
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener
import java.io.IOException

class RestRefreshSynonymAnalyzerAction : BaseRestHandler() {

    override fun getName(): String = "refresh_synonym_analyzer_action"

    override fun routes(): List<Route> {
        return listOf(
                Route(POST, REFRESH_SYNONYM_ANALYZER_URI)
        )
    }

    // TODO: Add indicesOptions?

    @Throws(IOException::class)
    @Suppress("SpreadOperator")
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val refreshSynonymAnalyzerRequest: RefreshSynonymAnalyzerRequest = RefreshSynonymAnalyzerRequest()
                .indices(*indices)

        return RestChannelConsumer { channel ->
            client.execute(RefreshSynonymAnalyzerAction.INSTANCE, refreshSynonymAnalyzerRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val REFRESH_SYNONYM_ANALYZER_URI = "$ANALYZER_BASE_URI/refresh_synonym_analyzer/{index}"
    }
}
