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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.Index

class UpdateManagedIndexMetaDataRequest : AcknowledgedRequest<UpdateManagedIndexMetaDataRequest> {
    lateinit var listOfIndexMetadata: List<Pair<Index, ManagedIndexMetaData>>
        private set

    constructor()

    constructor(listOfIndexMetadata: List<Pair<Index, ManagedIndexMetaData>>) {
        this.listOfIndexMetadata = listOfIndexMetadata
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (!this::listOfIndexMetadata.isInitialized) {
            validationException = addValidationError("must specify index List for UpdateManagedIndexMetaData", validationException)
        }
        return validationException
    }

    override fun writeTo(streamOutput: StreamOutput) {
        super.writeTo(streamOutput)
        streamOutput.writeCollection(listOfIndexMetadata) { so, pair ->
            pair.first.writeTo(so)
            pair.second.writeTo(so)
        }
    }

    override fun readFrom(streamInput: StreamInput) {
        super.readFrom(streamInput)

        listOfIndexMetadata = streamInput.readList {
            val index = Index(it)
            val managedIndexMetaData = ManagedIndexMetaData.fromStreamInput(it)
            Pair(index, managedIndexMetaData)
        }
    }
}
