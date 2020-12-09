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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_TEMPLATE_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put.PutISMTemplateAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put.PutISMTemplateRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put.PutISMTemplateResponse
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.PUT
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import org.elasticsearch.rest.action.RestToXContentListener
import java.lang.IllegalArgumentException
import java.time.Instant

private val log = LogManager.getLogger(RestAddISMTemplateAction::class.java)

// RestIndexPolicyAction
class RestAddISMTemplateAction : BaseRestHandler() {
    override fun routes(): List<Route> {
        return listOf(
            Route(PUT, ISM_TEMPLATE_BASE_URI),
            Route(PUT, "$ISM_TEMPLATE_BASE_URI/{templateID}")
        )
    }

    override fun getName(): String = "add_ism_template_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("${request.method()} $ISM_TEMPLATE_BASE_URI")

        val templateName = request.param("templateID", "")
        if (templateName == "") { throw IllegalArgumentException("Missing template name") }

        log.info("request content ${XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()}")

        val xcp = request.contentParser()
        // ISMTemplate.show(xcp)
        val ismTemplate = ISMTemplate.parse(xcp).copy(lastUpdatedTime = Instant.now())
        log.info("rest template $ismTemplate")

        val masterTimeout = request.paramAsTime("master_timeout", DEFAULT_MASTER_NODE_TIMEOUT)
        val addISMTemplateRequest = PutISMTemplateRequest(templateName, ismTemplate).masterNodeTimeout(masterTimeout)

        return RestChannelConsumer { channel ->
            client.execute(PutISMTemplateAction.INSTANCE, addISMTemplateRequest, object : RestResponseListener<PutISMTemplateResponse>(channel) {
                override fun buildResponse(response: PutISMTemplateResponse): RestResponse {
                    val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                    if (response.status == RestStatus.CREATED) {
                        val location = "$ISM_TEMPLATE_BASE_URI/${response.id}"
                        restResponse.addHeader("Location", location)
                    }
                    return restResponse
                }
            })
        }
    }
}
