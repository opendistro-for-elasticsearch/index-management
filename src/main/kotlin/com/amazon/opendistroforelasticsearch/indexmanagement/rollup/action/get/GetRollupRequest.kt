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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetRollupRequest : ActionRequest {
    var id: String
    val method: RestRequest.Method
    val srcContext: FetchSourceContext?

    constructor(
        id: String,
        method: RestRequest.Method,
        srcContext: FetchSourceContext?
    ) : super() {
        this.id = id
        this.method = method
        this.srcContext = srcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        method = sin.readEnum(RestRequest.Method::class.java),
        srcContext = if (sin.readBoolean()) FetchSourceContext(sin) else null
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (id.isBlank()) {
            validationException = ValidateActions.addValidationError("id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeEnum(method)
        if (srcContext == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        }
    }
}
