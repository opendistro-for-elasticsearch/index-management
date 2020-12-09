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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.MasterNodeRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput

// GetIndexTemplatesResponse
class GetISMTemplateRequest : MasterNodeRequest<GetISMTemplateRequest> {

    val templateNames: Array<String>

    constructor(sin: StreamInput) : super(sin) {
        templateNames = sin.readStringArray()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeStringArray(templateNames)
    }

    constructor(templateName: Array<String>) : super() {
        this.templateNames = templateName
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
