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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException

class IndexTransformRequest : IndexRequest {
    var transform: Transform

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        // Modifications to fix Transform are causing issues here because there are fewer reads to the StreamInput
        // than there are writes, so the Refresh Policy ends up reading the tail of the Transform, not the refresh policy
        transform = Transform(sin)
        super.setRefreshPolicy(WriteRequest.RefreshPolicy.readFrom(sin))
    }

    constructor(
        transform: Transform,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) {
        this.transform = transform
        if (transform.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || transform.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            this.opType(DocWriteRequest.OpType.CREATE)
        } else {
            this.setIfSeqNo(transform.seqNo)
                    .setIfPrimaryTerm(transform.primaryTerm)
        }
        super.setRefreshPolicy(refreshPolicy)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transform.id.isBlank()) {
            validationException = addValidationError("transformID is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        transform.writeTo(out)
        refreshPolicy.writeTo(out)
    }
}
