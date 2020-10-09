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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.explain

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainRollupRequest : ActionRequest {

    val rollupIDs: List<String>

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(rollupIDs = sin.readStringArray().toList())

    constructor(rollupIDs: List<String>) {
        this.rollupIDs = rollupIDs
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollupIDs.isEmpty()) {
            validationException = addValidationError("Missing rollupID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(rollupIDs.toTypedArray())
    }
}
