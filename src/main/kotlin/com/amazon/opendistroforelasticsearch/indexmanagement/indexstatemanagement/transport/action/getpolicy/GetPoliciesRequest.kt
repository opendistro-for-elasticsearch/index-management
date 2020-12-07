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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.SearchParams
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class GetPoliciesRequest : ActionRequest {

    val searchParams: SearchParams
    val index: String

    constructor(
        searchParams: SearchParams,
        index: String
    ) : super() {
        this.searchParams = searchParams
        this.index = index
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        searchParams = SearchParams(sin),
        index = sin.readString()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        searchParams.writeTo(out)
        out.writeString(index)
    }
}