/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import java.io.IOException

data class Params(
    val size: Int,
    val from: Int,
    val sortField: String,
    val sortOrder: String,
    val queryString: String
) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
        size = sin.readInt(),
        from = sin.readInt(),
        sortField = sin.readString(),
        sortOrder = sin.readString(),
        queryString = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInt(size)
        out.writeInt(from)
        out.writeString(sortField)
        out.writeString(sortOrder)
        out.writeString(queryString)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun fromStreamInput(sin: StreamInput): Params {
            return Params(sin)
        }
    }
}
