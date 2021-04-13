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
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview.PreviewTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestToXContentListener

class RestPreviewTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(POST, TRANSFORM_BASE_URI),
            RestHandler.Route(POST, "$TRANSFORM_BASE_URI/_preview")
        )
    }

    override fun getName(): String {
        return "opendistro_preview_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(parse = Transform.Companion::parse)
        val previewTransformRequest = PreviewTransformRequest(transform)
        return RestChannelConsumer { channel ->
            client.execute(PreviewTransformAction.INSTANCE, previewTransformRequest, RestToXContentListener(channel))
        }
    }
}
