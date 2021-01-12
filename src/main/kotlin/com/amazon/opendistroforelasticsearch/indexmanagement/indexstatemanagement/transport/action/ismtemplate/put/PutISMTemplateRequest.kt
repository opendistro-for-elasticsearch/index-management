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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class PutISMTemplateRequest : MasterNodeRequest<PutISMTemplateRequest> {

    val templateName: String
    val ismTemplate: ISMTemplate

    constructor(
        templateName: String,
        ismTemplate: ISMTemplate
    ) : super() {
        this.templateName = templateName
        this.ismTemplate = ismTemplate
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        templateName = sin.readString()
        ismTemplate = ISMTemplate(sin)
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(templateName)
        ismTemplate.writeTo(out)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
