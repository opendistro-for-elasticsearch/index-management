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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import java.time.Instant
import java.util.Locale

abstract class Step(val name: String, val managedIndexMetaData: ManagedIndexMetaData) {

    abstract suspend fun execute()

    abstract fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData

    fun getStartingStepMetaData(): StepMetaData {
        return StepMetaData(name, getStepStartTime().toEpochMilli(), StepStatus.STARTING)
    }

    fun getStepStartTime(): Instant {
        if (managedIndexMetaData.stepMetaData == null || managedIndexMetaData.stepMetaData.name != this.name) {
            return Instant.now()
        }
        return Instant.ofEpochMilli(managedIndexMetaData.stepMetaData.startTime)
    }

    enum class StepStatus(val status: String) : Writeable {
        STARTING("starting"),
        CONDITION_NOT_MET("condition_not_met"),
        FAILED("failed"),
        COMPLETED("completed");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): StepStatus {
                return valueOf(streamInput.readString().toUpperCase(Locale.ROOT))
            }
        }
    }
}
