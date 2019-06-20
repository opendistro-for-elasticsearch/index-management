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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder

data class ManagedIndexMetaData(
    val index: String,
    val indexUuid: String,
    val policyName: String,
    val policyVersion: String,
    val state: String,
    val stateStartTime: String,
    val actionIndex: String,
    val action: String,
    val actionStartTime: String,
    val step: String,
    val stepStartTime: String,
    val failedStep: String?
) : Writeable, ToXContentFragment {

    companion object {
        const val MANAGED_INDEX_METADATA = "managed_index_metadata"

        const val INDEX = "index"
        const val INDEX_UUID = "index_uuid"
        const val POLICY_NAME = "policy_name"
        const val POLICY_VERSION = "policy_version"
        const val STATE = "state"
        const val STATE_START_TIME = "state_start_time"
        const val ACTION_INDEX = "action_index"
        const val ACTION = "action"
        const val ACTION_START_TIME = "action_start_time"
        const val STEP = "step"
        const val STEP_START_TIME = "step_start_time"
        const val FAILED_STEP = "failed_step"

        fun fromMap(index: String, indexUuid: String, map: Map<String, String>): ManagedIndexMetaData {
            return ManagedIndexMetaData(
                index,
                indexUuid,
                requireNotNull(map[POLICY_NAME]) { "$POLICY_NAME is null" },
                requireNotNull(map[POLICY_VERSION]) { "$POLICY_VERSION is null" },
                requireNotNull(map[STATE]) { "$STATE is null" },
                requireNotNull(map[STATE_START_TIME]) { "$STATE_START_TIME is null" },
                requireNotNull(map[ACTION_INDEX]) { "$ACTION_INDEX is null" },
                requireNotNull(map[ACTION]) { "$ACTION is null" },
                requireNotNull(map[ACTION_START_TIME]) { "$ACTION_START_TIME is null" },
                requireNotNull(map[STEP]) { "$STEP is null" },
                requireNotNull(map[STEP_START_TIME]) { "$STEP_START_TIME is null" },
                map[FAILED_STEP]
            )
        }

        fun fromStreamInput(si: StreamInput): ManagedIndexMetaData {
            val index: String? = si.readString()
            val indexUuid: String? = si.readString()
            val policyName: String? = si.readString()
            val policyVersion: String? = si.readString()
            val state: String? = si.readString()
            val stateStartTime: String? = si.readString()
            val actionIndex: String? = si.readString()
            val action: String? = si.readString()
            val actionStartTime: String? = si.readString()
            val step: String? = si.readString()
            val stepStartTime: String? = si.readString()
            val failedStep: String? = si.readOptionalString()

            return ManagedIndexMetaData(
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(indexUuid) { "$INDEX_UUID is null" },
                requireNotNull(policyName) { "$POLICY_NAME is null" },
                requireNotNull(policyVersion) { "$POLICY_VERSION is null" },
                requireNotNull(state) { "$STATE is null" },
                requireNotNull(stateStartTime) { "$STATE_START_TIME is null" },
                requireNotNull(actionIndex) { "$ACTION_INDEX is null" },
                requireNotNull(action) { "$ACTION is null" },
                requireNotNull(actionStartTime) { "$ACTION_START_TIME is null" },
                requireNotNull(step) { "$STEP is null" },
                requireNotNull(stepStartTime) { "$STEP_START_TIME is null" },
                failedStep
            )
        }
    }

    fun toMap(): Map<String, String?> {
        return mapOf(
            POLICY_NAME to policyName,
            POLICY_VERSION to policyVersion,
            STATE to state,
            STATE_START_TIME to stateStartTime,
            ACTION_INDEX to actionIndex,
            ACTION to action,
            ACTION_START_TIME to actionStartTime,
            STEP to step,
            STEP_START_TIME to stepStartTime,
            FAILED_STEP to failedStep
        )
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.field(INDEX_UUID, indexUuid)
            .field(POLICY_NAME, policyName)
            .field(POLICY_VERSION, policyVersion)
            .field(STATE, state)
            .field(STATE_START_TIME, stateStartTime)
            .field(ACTION_INDEX, actionIndex)
            .field(ACTION, action)
            .field(ACTION_START_TIME, actionStartTime)
            .field(STEP, step)
            .field(STEP_START_TIME, stepStartTime)
            .field(FAILED_STEP, failedStep)
    }

    override fun writeTo(streamOutput: StreamOutput) {
        streamOutput.writeString(index)
        streamOutput.writeString(indexUuid)
        streamOutput.writeString(policyName)
        streamOutput.writeString(policyVersion)
        streamOutput.writeString(state)
        streamOutput.writeString(stateStartTime)
        streamOutput.writeString(actionIndex)
        streamOutput.writeString(action)
        streamOutput.writeString(actionStartTime)
        streamOutput.writeString(step)
        streamOutput.writeString(stepStartTime)
        streamOutput.writeOptionalString(failedStep)
    }
}
