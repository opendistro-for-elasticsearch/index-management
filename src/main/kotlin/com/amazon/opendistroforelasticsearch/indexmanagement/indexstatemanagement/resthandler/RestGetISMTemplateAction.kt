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

import org.apache.logging.log4j.LogManager
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_TEMPLATE_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get.GetISMTemplateAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get.GetISMTemplateRequest
import org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestGetISMTemplateAction::class.java)

class RestGetISMTemplateAction : BaseRestHandler() {
    override fun routes(): List<Route> {
        return listOf(
            Route(GET, ISM_TEMPLATE_BASE_URI),
            Route(GET, "$ISM_TEMPLATE_BASE_URI/{templateID}")
        )
    }

    override fun getName(): String = "get_ism_template_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} $ISM_TEMPLATE_BASE_URI")

        val templateNames = Strings.splitStringByCommaToArray(request.param("templateID"))
        val masterTimeout = request.paramAsTime("master_timeout", DEFAULT_MASTER_NODE_TIMEOUT)
        val getISMTemplateReq = GetISMTemplateRequest(templateNames).masterNodeTimeout(masterTimeout)

        return RestChannelConsumer { channel ->
            client.execute(GetISMTemplateAction.INSTANCE, getISMTemplateReq, RestToXContentListener(channel))
        }
    }
}
