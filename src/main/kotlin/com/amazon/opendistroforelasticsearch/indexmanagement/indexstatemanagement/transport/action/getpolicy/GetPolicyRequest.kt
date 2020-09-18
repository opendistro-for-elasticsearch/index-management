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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetPolicyRequest : ActionRequest {

    val policyID: String?
    val version: Long
    val fetchSrcContext: FetchSourceContext?

    constructor(
        policyID: String?,
        version: Long,
        fetchSrcContext: FetchSourceContext?
    ) : super() {
        this.policyID = policyID
        this.version = version
        this.fetchSrcContext = fetchSrcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        version = sin.readLong(),
        fetchSrcContext = sin.readOptionalWriteable(::FetchSourceContext)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (policyID == null || policyID.isEmpty()) {
            validationException = ValidateActions.addValidationError(
                    "Missing policy ID",
                    validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
        out.writeLong(version)
        out.writeOptionalWriteable(fetchSrcContext)
    }
}
