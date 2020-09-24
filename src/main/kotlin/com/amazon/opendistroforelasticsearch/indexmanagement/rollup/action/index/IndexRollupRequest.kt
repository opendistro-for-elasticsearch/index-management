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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException

class IndexRollupRequest : IndexRequest {
    val rollupID: String
    val rollup: Rollup

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        rollupID = sin.readString()
        rollup = Rollup(sin)
    }

    constructor(
        rollupID: String,
        rollup: Rollup,
        seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
        primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        refreshPolicy: WriteRequest.RefreshPolicy = WriteRequest.RefreshPolicy.NONE
    ) {
        this.rollupID = rollupID
        this.rollup = rollup
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.opType(DocWriteRequest.OpType.CREATE)
        } else {
            this.setIfSeqNo(seqNo)
                .setIfPrimaryTerm(primaryTerm)
        }
        super.setRefreshPolicy(refreshPolicy)
    }

    // TODO
    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollupID.isBlank()) {
            validationException = addValidationError("rollupID is missing", validationException)
        }
        return validationException
    }

    fun rollupID(): String = rollupID

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(rollupID)
        rollup.writeTo(out)
    }
}
