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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.json.JsonXContent

data class ManagedIndexMetaData(
    val index: String,
    val indexUuid: String,
    val policyName: String,
    val policySeqNo: Long?,
    val policyPrimaryTerm: Long?,
    val state: String?,
    val stateStartTime: Long?,
    val transitionTo: String?,
    val actionIndex: Int?,
    val action: String?,
    val actionStartTime: Long?,
    val step: String?,
    val stepStartTime: Long?,
    val stepCompleted: Boolean?,
    val failed: Boolean?,
    val policyCompleted: Boolean?,
    val rolledOver: Boolean?,
    val info: Map<String, Any>?,
    val consumedRetries: Int?
) : Writeable, ToXContentFragment {

    fun toMap(): Map<String, String?> {
        return mapOf(
            INDEX to index,
            INDEX_UUID to indexUuid,
            POLICY_NAME to policyName,
            POLICY_SEQ_NO to policySeqNo?.toString(),
            POLICY_PRIMARY_TERM to policyPrimaryTerm?.toString(),
            STATE to state,
            STATE_START_TIME to stateStartTime?.toString(),
            TRANSITION_TO to transitionTo,
            ACTION_INDEX to actionIndex?.toString(),
            ACTION to action,
            ACTION_START_TIME to actionStartTime?.toString(),
            STEP to step,
            STEP_START_TIME to stepStartTime?.toString(),
            STEP_COMPLETED to stepCompleted?.toString(),
            FAILED to failed?.toString(),
            POLICY_COMPLETED to policyCompleted?.toString(),
            ROLLED_OVER to rolledOver?.toString(),
            INFO to info?.let { Strings.toString(XContentFactory.jsonBuilder().map(it)) },
            CONSUMED_RETRIES to consumedRetries?.toString()
        )
    }

    @Suppress("ComplexMethod")
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        // The order we check values matters here as we are only trying to show what is needed for the customer
        // and can return early on certain checks like policyCompleted
        builder
            .field(INDEX, index)
            .field(INDEX_UUID, indexUuid)
            .field(POLICY_NAME, policyName)

        if (policySeqNo != null) builder.field(POLICY_SEQ_NO, policySeqNo)
        if (policyPrimaryTerm != null) builder.field(POLICY_PRIMARY_TERM, policyPrimaryTerm)
        if (policyCompleted == true) {
            builder.field(POLICY_COMPLETED, policyCompleted)
            return builder
        }

        // Only show rolled_over if we have rolled over or we are in the rollover action
        if (rolledOver == true || action == ActionConfig.ActionType.ROLLOVER.type) {
            builder.field(ROLLED_OVER, rolledOver)
        }

        if (transitionTo != null) {
            builder.field(TRANSITION_TO, transitionTo)
        } else {
            if (state != null) builder.field(STATE, state)
            if (stateStartTime != null) builder.field(STATE_START_TIME, stateStartTime)
            if (actionIndex != null) builder.field(ACTION_INDEX, actionIndex)
            if (action != null) builder.field(ACTION, action)
            if (actionStartTime != null) builder.field(ACTION_START_TIME, actionStartTime)
            if (step != null) builder.field(STEP, step)
            if (stepStartTime != null) builder.field(STEP_START_TIME, stepStartTime)
            if (stepCompleted != null) builder.field(STEP_COMPLETED, stepCompleted)
            if (consumedRetries != null) builder.field(CONSUMED_RETRIES, consumedRetries)
        }

        if (failed != null) builder.field(FAILED, failed)
        if (info != null) builder.field(INFO, info)
        return builder
    }

    override fun writeTo(streamOutput: StreamOutput) {
        streamOutput.writeString(index)
        streamOutput.writeString(indexUuid)
        streamOutput.writeString(policyName)
        streamOutput.writeOptionalLong(policySeqNo)
        streamOutput.writeOptionalLong(policyPrimaryTerm)
        streamOutput.writeOptionalString(state)
        streamOutput.writeOptionalLong(stateStartTime)
        streamOutput.writeOptionalString(transitionTo)
        streamOutput.writeOptionalInt(actionIndex)
        streamOutput.writeOptionalString(action)
        streamOutput.writeOptionalLong(actionStartTime)
        streamOutput.writeOptionalString(step)
        streamOutput.writeOptionalLong(stepStartTime)
        streamOutput.writeOptionalBoolean(stepCompleted)
        streamOutput.writeOptionalBoolean(failed)
        streamOutput.writeOptionalBoolean(policyCompleted)
        streamOutput.writeOptionalBoolean(rolledOver)
        streamOutput.writeOptionalString(info?.toString())
        streamOutput.writeOptionalInt(consumedRetries)
    }

    companion object {
        const val MANAGED_INDEX_METADATA = "managed_index_metadata"

        const val INDEX = "index"
        const val INDEX_UUID = "index_uuid"
        const val POLICY_NAME = "policy_name"
        const val POLICY_SEQ_NO = "policy_seq_no"
        const val POLICY_PRIMARY_TERM = "policy_primary_term"
        const val STATE = "state"
        const val STATE_START_TIME = "state_start_time"
        const val TRANSITION_TO = "transition_to"
        const val ACTION_INDEX = "action_index"
        const val ACTION = "action"
        const val ACTION_START_TIME = "action_start_time"
        const val STEP = "step"
        const val STEP_START_TIME = "step_start_time"
        const val STEP_COMPLETED = "step_completed"
        const val FAILED = "failed"
        const val POLICY_COMPLETED = "policy_completed"
        const val ROLLED_OVER = "rolled_over"
        const val INFO = "info"
        const val CONSUMED_RETRIES = "consumed_retries"

        fun fromStreamInput(si: StreamInput): ManagedIndexMetaData {
            val index: String? = si.readString()
            val indexUuid: String? = si.readString()
            val policyName: String? = si.readString()
            val policySeqNo: Long? = si.readOptionalLong()
            val policyPrimaryTerm: Long? = si.readOptionalLong()
            val state: String? = si.readOptionalString()
            val stateStartTime: Long? = si.readOptionalLong()
            val transitionTo: String? = si.readOptionalString()
            val actionIndex: Int? = si.readOptionalInt()
            val action: String? = si.readOptionalString()
            val actionStartTime: Long? = si.readOptionalLong()
            val step: String? = si.readOptionalString()
            val stepStartTime: Long? = si.readOptionalLong()
            val stepCompleted: Boolean? = si.readOptionalBoolean()
            val failed: Boolean? = si.readOptionalBoolean()
            val policyCompleted: Boolean? = si.readOptionalBoolean()
            val rolledOver: Boolean? = si.readOptionalBoolean()
            val info = si.readOptionalString()?.let { XContentHelper.convertToMap(JsonXContent.jsonXContent, it, false) }?.toMap()
            val consumedRetries: Int? = si.readOptionalInt()

            return ManagedIndexMetaData(
                index = requireNotNull(index) { "$INDEX is null" },
                indexUuid = requireNotNull(indexUuid) { "$INDEX_UUID is null" },
                policyName = requireNotNull(policyName) { "$POLICY_NAME is null" },
                policySeqNo = policySeqNo,
                policyPrimaryTerm = policyPrimaryTerm,
                state = state,
                stateStartTime = stateStartTime,
                transitionTo = transitionTo,
                actionIndex = actionIndex,
                action = action,
                actionStartTime = actionStartTime,
                step = step,
                stepStartTime = stepStartTime,
                stepCompleted = stepCompleted,
                failed = failed,
                policyCompleted = policyCompleted,
                rolledOver = rolledOver,
                info = info,
                consumedRetries = consumedRetries
            )
        }

        fun fromMap(map: Map<String, String?>): ManagedIndexMetaData {
            return ManagedIndexMetaData(
                requireNotNull(map[INDEX]) { "$INDEX is null" },
                requireNotNull(map[INDEX_UUID]) { "$INDEX_UUID is null" },
                requireNotNull(map[POLICY_NAME]) { "$POLICY_NAME is null" },
                map[POLICY_SEQ_NO]?.toLong(),
                map[POLICY_PRIMARY_TERM]?.toLong(),
                map[STATE],
                map[STATE_START_TIME]?.toLong(),
                map[TRANSITION_TO],
                map[ACTION_INDEX]?.toInt(),
                map[ACTION],
                map[ACTION_START_TIME]?.toLong(),
                map[STEP],
                map[STEP_START_TIME]?.toLong(),
                map[STEP_COMPLETED]?.toBoolean(),
                map[FAILED]?.toBoolean(),
                map[POLICY_COMPLETED]?.toBoolean(),
                map[ROLLED_OVER]?.toBoolean(),
                map[INFO]?.let { XContentHelper.convertToMap(JsonXContent.jsonXContent, it, false) },
                map[CONSUMED_RETRIES]?.toInt()
            )
        }
    }
}
