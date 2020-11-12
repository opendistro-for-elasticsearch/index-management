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
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetRollupRequest : ActionRequest {
    var id: String
    val srcContext: FetchSourceContext?
    val preference: String?

    constructor(
        id: String,
        srcContext: FetchSourceContext? = null,
        preference: String? = null
    ) : super() {
        this.id = id
        this.srcContext = srcContext
        this.preference = preference
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        srcContext = if (sin.readBoolean()) FetchSourceContext(sin) else null,
        preference = sin.readOptionalString()
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
        if (srcContext == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        }
        out.writeOptionalString(preference)
    }
}
