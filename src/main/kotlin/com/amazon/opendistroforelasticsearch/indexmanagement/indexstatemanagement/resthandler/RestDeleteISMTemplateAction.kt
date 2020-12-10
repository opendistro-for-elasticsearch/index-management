/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_TEMPLATE_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.delete.DeleteISMTemplateAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.delete.DeleteISMTemplateRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.DELETE
import org.elasticsearch.rest.action.RestToXContentListener
import java.lang.IllegalArgumentException

private val log = LogManager.getLogger(RestDeleteISMTemplateAction::class.java)

class RestDeleteISMTemplateAction : BaseRestHandler() {
    override fun routes(): List<Route> {
        return listOf(
            Route(DELETE, "$ISM_TEMPLATE_BASE_URI/{templateID}")
        )
    }

    override fun getName(): String = "remove_ism_template_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} $ISM_TEMPLATE_BASE_URI")

        val templateName = request.param("templateID", "")
        if (templateName == "") { throw IllegalArgumentException("Missing template name") }

        val deleteISMTemplateRequest = DeleteISMTemplateRequest(templateName)

        return RestChannelConsumer { channel ->
            client.execute(DeleteISMTemplateAction.INSTANCE, deleteISMTemplateRequest, RestToXContentListener(channel))
        }
    }
}
