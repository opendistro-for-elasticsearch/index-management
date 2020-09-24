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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import java.io.IOException
import java.time.Instant

class StartRollupRequest : UpdateRequest {
    val id: String

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        id = sin.readString()
    }

    constructor(rollupID: String) {
        this.id = rollupID
        val now = Instant.now().toEpochMilli()
        super.index(INDEX_MANAGEMENT_INDEX)
            .id(rollupID)
            .doc(mapOf(Rollup.ROLLUP_TYPE to mapOf(Rollup.ENABLED_FIELD to true,
                Rollup.ENABLED_TIME_FIELD to now, Rollup.LAST_UPDATED_TIME_FIELD to now)))
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (id.isBlank()) {
            validationException = addValidationError("id is missing", validationException)
        }
        return validationException
    }

    fun rollupID(): String = id

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(id)
    }
}