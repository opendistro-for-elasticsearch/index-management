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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
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
    val policyID: String,
    val policySeqNo: Long?,
    val policyPrimaryTerm: Long?,
    val policyCompleted: Boolean?,
    val rolledOver: Boolean?,
    val wasReadOnly: Boolean?,
    val transitionTo: String?,
    val stateMetaData: StateMetaData?,
    val actionMetaData: ActionMetaData?,
    val stepMetaData: StepMetaData?,
    val policyRetryInfo: PolicyRetryInfoMetaData?,
    val info: Map<String, Any>?
) : Writeable, ToXContentFragment {

    fun toMap(): Map<String, String?> {
        return mapOf(
            INDEX to index,
            INDEX_UUID to indexUuid,
            POLICY_ID to policyID,
            POLICY_SEQ_NO to policySeqNo?.toString(),
            POLICY_PRIMARY_TERM to policyPrimaryTerm?.toString(),
            POLICY_COMPLETED to policyCompleted?.toString(),
            ROLLED_OVER to rolledOver?.toString(),
            WAS_READ_ONLY to wasReadOnly?.toString(),
            TRANSITION_TO to transitionTo,
            StateMetaData.STATE to stateMetaData?.getMapValueString(),
            ActionMetaData.ACTION to actionMetaData?.getMapValueString(),
            StepMetaData.STEP to stepMetaData?.getMapValueString(),
            PolicyRetryInfoMetaData.RETRY_INFO to policyRetryInfo?.getMapValueString(),
            INFO to info?.let { Strings.toString(XContentFactory.jsonBuilder().map(it)) }
        )
    }

    @Suppress("ComplexMethod")
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        // The order we check values matters here as we are only trying to show what is needed for the customer
        // and can return early on certain checks like policyCompleted
        builder
            .field(INDEX, index)
            .field(INDEX_UUID, indexUuid)
            .field(POLICY_ID, policyID)

        if (policySeqNo != null) builder.field(POLICY_SEQ_NO, policySeqNo)
        if (policyPrimaryTerm != null) builder.field(POLICY_PRIMARY_TERM, policyPrimaryTerm)

        // Only show rolled_over if we have rolled over or we are in the rollover action
        if (rolledOver == true || (actionMetaData != null && actionMetaData.name == ActionConfig.ActionType.ROLLOVER.type)) {
            builder.field(ROLLED_OVER, rolledOver)
        }

        // Only show was_read_only if we are on the force_merge action
        if (actionMetaData != null && actionMetaData.name == ActionConfig.ActionType.FORCE_MERGE.type) {
            builder.field(WAS_READ_ONLY, wasReadOnly)
        }

        if (policyCompleted == true) {
            builder.field(POLICY_COMPLETED, policyCompleted)
            return builder
        }

        val transitionToExists = transitionTo != null

        if (transitionToExists) {
            builder.field(TRANSITION_TO, transitionTo)
        }

        if (stateMetaData != null && !transitionToExists) {
            builder.startObject(StateMetaData.STATE)
            stateMetaData.toXContent(builder, params)
            builder.endObject()
        }

        if (actionMetaData != null && !transitionToExists) {
            builder.startObject(ActionMetaData.ACTION)
            actionMetaData.toXContent(builder, params)
            builder.endObject()
        }

        if (policyRetryInfo != null && !transitionToExists) {
            builder.startObject(PolicyRetryInfoMetaData.RETRY_INFO)
            policyRetryInfo.toXContent(builder, params)
            builder.endObject()
        }

        if (info != null) builder.field(INFO, info)
        return builder
    }

    override fun writeTo(streamOutput: StreamOutput) {
        streamOutput.writeString(index)
        streamOutput.writeString(indexUuid)
        streamOutput.writeString(policyID)
        streamOutput.writeOptionalLong(policySeqNo)
        streamOutput.writeOptionalLong(policyPrimaryTerm)
        streamOutput.writeOptionalBoolean(policyCompleted)
        streamOutput.writeOptionalBoolean(rolledOver)
        streamOutput.writeOptionalBoolean(wasReadOnly)
        streamOutput.writeOptionalString(transitionTo)

        streamOutput.writeOptionalWriteable { stateMetaData?.writeTo(it) }
        streamOutput.writeOptionalWriteable { actionMetaData?.writeTo(it) }
        streamOutput.writeOptionalWriteable { stepMetaData?.writeTo(it) }
        streamOutput.writeOptionalWriteable { policyRetryInfo?.writeTo(it) }

        streamOutput.writeOptionalString(info?.toString())
    }

    companion object {
        const val MANAGED_INDEX_METADATA = "managed_index_metadata"

        const val NAME = "name"
        const val START_TIME = "start_time"

        const val INDEX = "index"
        const val INDEX_UUID = "index_uuid"
        const val POLICY_ID = "policy_id"
        const val POLICY_SEQ_NO = "policy_seq_no"
        const val POLICY_PRIMARY_TERM = "policy_primary_term"
        const val POLICY_COMPLETED = "policy_completed"
        const val ROLLED_OVER = "rolled_over"
        const val WAS_READ_ONLY = "was_read_only"
        const val TRANSITION_TO = "transition_to"
        const val INFO = "info"

        fun fromStreamInput(si: StreamInput): ManagedIndexMetaData {
            val index: String? = si.readString()
            val indexUuid: String? = si.readString()
            val policyID: String? = si.readString()
            val policySeqNo: Long? = si.readOptionalLong()
            val policyPrimaryTerm: Long? = si.readOptionalLong()
            val policyCompleted: Boolean? = si.readOptionalBoolean()
            val rolledOver: Boolean? = si.readOptionalBoolean()
            val wasReadOnly: Boolean? = si.readOptionalBoolean()
            val transitionTo: String? = si.readOptionalString()

            val state: StateMetaData? = si.readOptionalWriteable { StateMetaData.fromStreamInput(it) }
            val action: ActionMetaData? = si.readOptionalWriteable { ActionMetaData.fromStreamInput(it) }
            val step: StepMetaData? = si.readOptionalWriteable { StepMetaData.fromStreamInput(it) }
            val retryInfo: PolicyRetryInfoMetaData? = si.readOptionalWriteable { PolicyRetryInfoMetaData.fromStreamInput(it) }

            val info = si.readOptionalString()?.let { XContentHelper.convertToMap(JsonXContent.jsonXContent, it, false) }?.toMap()

            return ManagedIndexMetaData(
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(indexUuid) { "$INDEX_UUID is null" },
                requireNotNull(policyID) { "$POLICY_ID is null" },
                policySeqNo,
                policyPrimaryTerm,
                policyCompleted,
                rolledOver,
                wasReadOnly,
                transitionTo,
                state,
                action,
                step,
                retryInfo,
                info
            )
        }

        fun fromMap(map: Map<String, String?>): ManagedIndexMetaData {

            return ManagedIndexMetaData(
                requireNotNull(map[INDEX]) { "$INDEX is null" },
                requireNotNull(map[INDEX_UUID]) { "$INDEX_UUID is null" },
                requireNotNull(map[POLICY_ID]) { "$POLICY_ID is null" },
                map[POLICY_SEQ_NO]?.toLong(),
                map[POLICY_PRIMARY_TERM]?.toLong(),
                map[POLICY_COMPLETED]?.toBoolean(),
                map[ROLLED_OVER]?.toBoolean(),
                map[WAS_READ_ONLY]?.toBoolean(),
                map[TRANSITION_TO],
                StateMetaData.fromManagedIndexMetaDataMap(map),
                ActionMetaData.fromManagedIndexMetaDataMap(map),
                StepMetaData.fromManagedIndexMetaDataMap(map),
                PolicyRetryInfoMetaData.fromManagedIndexMetaDataMap(map),
                map[INFO]?.let { XContentHelper.convertToMap(JsonXContent.jsonXContent, it, false) }
            )
        }
    }
}
