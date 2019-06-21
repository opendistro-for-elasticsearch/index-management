/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.Index

class UpdateManagedIndexMetaDataRequest : AcknowledgedRequest<UpdateManagedIndexMetaDataRequest> {
    lateinit var index: Index
        private set
    lateinit var managedIndexMetaData: ManagedIndexMetaData
        private set

    constructor()

    constructor(index: Index, managedIndexMetaData: ManagedIndexMetaData) {
        this.index = index
        this.managedIndexMetaData = managedIndexMetaData
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (!this::index.isInitialized) {
            validationException = addValidationError("must specify index for UpdateManagedIndexMetaData", validationException)
        } else if (!this::managedIndexMetaData.isInitialized) {
            validationException = addValidationError("must specify managedIndexMetaData for UpdateManagedIndexMetaData", validationException)
        }
        return validationException
    }

    override fun writeTo(streamOutput: StreamOutput) {
        super.writeTo(streamOutput)
        index.writeTo(streamOutput)
        managedIndexMetaData.writeTo(streamOutput)
    }

    override fun readFrom(streamInput: StreamInput) {
        super.readFrom(streamInput)
        index = Index(streamInput)
        managedIndexMetaData = ManagedIndexMetaData.fromStreamInput(streamInput)
    }
}
