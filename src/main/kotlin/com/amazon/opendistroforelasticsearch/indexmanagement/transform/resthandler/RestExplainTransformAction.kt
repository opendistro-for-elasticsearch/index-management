/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain.ExplainTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain.ExplainTransformRequest
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

class RestExplainTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(Route(GET, "$TRANSFORM_BASE_URI/{transformID}/_explain"))
    }

    override fun getName(): String = "opendistro_explain_transform_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformIDs: List<String> = Strings.splitStringByCommaToArray(request.param("transformID")).toList()
        if (transformIDs.isEmpty()) {
            throw IllegalArgumentException("Missing transformID")
        }
        val explainRequest = ExplainTransformRequest(transformIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainTransformAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
