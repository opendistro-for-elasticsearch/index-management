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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.addObject
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.index.seqno.SequenceNumbers
import java.io.IOException

data class ManagedIndexMetaData(
    val index: String,
    val indexUuid: String,
    val policyID: String,
    val policySeqNo: Long?,
    val policyPrimaryTerm: Long?,
    val policyCompleted: Boolean?,
    val rolledOver: Boolean?,
    val transitionTo: String?,
    val stateMetaData: StateMetaData?,
    val actionMetaData: ActionMetaData?,
    val stepMetaData: StepMetaData?,
    val policyRetryInfo: PolicyRetryInfoMetaData?,
    val info: Map<String, Any>?,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
) : Writeable, ToXContentFragment {

    @Suppress("ComplexMethod")
    fun toMap(): Map<String, String> {
        val resultMap = mutableMapOf<String, String> ()
        resultMap[INDEX] = index
        resultMap[INDEX_UUID] = indexUuid
        resultMap[POLICY_ID] = policyID
        if (policySeqNo != null) resultMap[POLICY_SEQ_NO] = policySeqNo.toString()
        if (policyPrimaryTerm != null) resultMap[POLICY_PRIMARY_TERM] = policyPrimaryTerm.toString()
        if (policyCompleted != null) resultMap[POLICY_COMPLETED] = policyCompleted.toString()
        if (rolledOver != null) resultMap[ROLLED_OVER] = rolledOver.toString()
        if (transitionTo != null) resultMap[TRANSITION_TO] = transitionTo
        if (stateMetaData != null) resultMap[StateMetaData.STATE] = stateMetaData.getMapValueString()
        if (actionMetaData != null) resultMap[ActionMetaData.ACTION] = actionMetaData.getMapValueString()
        if (stepMetaData != null) resultMap[StepMetaData.STEP] = stepMetaData.getMapValueString()
        if (policyRetryInfo != null) resultMap[PolicyRetryInfoMetaData.RETRY_INFO] = policyRetryInfo.getMapValueString()
        if (info != null) resultMap[INFO] = Strings.toString(XContentFactory.jsonBuilder().map(info))

        return resultMap
    }

    fun toXContent(builder: XContentBuilder, params: ToXContent.Params, forIndex: Boolean): XContentBuilder {
        // forIndex means saving to config index, distinguish from Explain and History, which only save meaningful partial metadata
        if (!forIndex) return toXContent(builder, params)

        builder
            .startObject()
                .startObject(MANAGED_INDEX_METADATA_TYPE)
                    .field(INDEX, index)
                    .field(INDEX_UUID, indexUuid)
                    .field(POLICY_ID, policyID)
                    .field(POLICY_SEQ_NO, policySeqNo)
                    .field(POLICY_PRIMARY_TERM, policyPrimaryTerm)
                    .field(POLICY_COMPLETED, policyCompleted)
                    .field(ROLLED_OVER, rolledOver)
                    .field(TRANSITION_TO, transitionTo)
                    .addObject(StateMetaData.STATE, stateMetaData, params, true)
                    .addObject(ActionMetaData.ACTION, actionMetaData, params, true)
                    .addObject(StepMetaData.STEP, stepMetaData, params, true)
                    .addObject(PolicyRetryInfoMetaData.RETRY_INFO, policyRetryInfo, params, true)
                    .field(INFO, info)
                .endObject()
            .endObject()
        return builder
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

        if (policyCompleted == true) {
            builder.field(POLICY_COMPLETED, policyCompleted)
            return builder
        }

        val transitionToExists = transitionTo != null
        if (transitionToExists) {
            builder.field(TRANSITION_TO, transitionTo)
        } else {
            builder.addObject(StateMetaData.STATE, stateMetaData, params)
                .addObject(ActionMetaData.ACTION, actionMetaData, params)
                .addObject(StepMetaData.STEP, stepMetaData, params)
        }
        builder.addObject(PolicyRetryInfoMetaData.RETRY_INFO, policyRetryInfo, params)

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
        streamOutput.writeOptionalString(transitionTo)

        streamOutput.writeOptionalWriteable(stateMetaData)
        streamOutput.writeOptionalWriteable(actionMetaData)
        streamOutput.writeOptionalWriteable(stepMetaData)
        streamOutput.writeOptionalWriteable(policyRetryInfo)

        if (info == null) {
            streamOutput.writeBoolean(false)
        } else {
            streamOutput.writeBoolean(true)
            streamOutput.writeMap(info)
        }
    }

    companion object {
        const val NO_ID = ""
        const val MANAGED_INDEX_METADATA_TYPE = "managed_index_metadata"

        const val NAME = "name"
        const val START_TIME = "start_time"

        const val INDEX = "index"
        const val INDEX_UUID = "index_uuid"
        const val POLICY_ID = "policy_id"
        const val POLICY_SEQ_NO = "policy_seq_no"
        const val POLICY_PRIMARY_TERM = "policy_primary_term"
        const val POLICY_COMPLETED = "policy_completed"
        const val ROLLED_OVER = "rolled_over"
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
            val transitionTo: String? = si.readOptionalString()

            val state: StateMetaData? = si.readOptionalWriteable { StateMetaData.fromStreamInput(it) }
            val action: ActionMetaData? = si.readOptionalWriteable { ActionMetaData.fromStreamInput(it) }
            val step: StepMetaData? = si.readOptionalWriteable { StepMetaData.fromStreamInput(it) }
            val retryInfo: PolicyRetryInfoMetaData? = si.readOptionalWriteable { PolicyRetryInfoMetaData.fromStreamInput(it) }

            val info = if (si.readBoolean()) {
                si.readMap()
            } else {
                null
            }

            return ManagedIndexMetaData(
                index = requireNotNull(index) { "$INDEX is null" },
                indexUuid = requireNotNull(indexUuid) { "$INDEX_UUID is null" },
                policyID = requireNotNull(policyID) { "$POLICY_ID is null" },
                policySeqNo = policySeqNo,
                policyPrimaryTerm = policyPrimaryTerm,
                policyCompleted = policyCompleted,
                rolledOver = rolledOver,
                transitionTo = transitionTo,
                stateMetaData = state,
                actionMetaData = action,
                stepMetaData = step,
                policyRetryInfo = retryInfo,
                info = info
            )
        }

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): ManagedIndexMetaData {
            var index: String? = null
            var indexUuid: String? = null
            var policyID: String? = null
            var policySeqNo: Long? = null
            var policyPrimaryTerm: Long? = null
            var policyCompleted: Boolean? = null
            var rolledOver: Boolean? = null
            var transitionTo: String? = null

            var state: StateMetaData? = null
            var action: ActionMetaData? = null
            var step: StepMetaData? = null
            var retryInfo: PolicyRetryInfoMetaData? = null

            var info: Map<String, Any>? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    INDEX -> index = xcp.text()
                    INDEX_UUID -> indexUuid = xcp.text()
                    POLICY_ID -> policyID = xcp.text()
                    POLICY_SEQ_NO -> policySeqNo = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    POLICY_PRIMARY_TERM -> policyPrimaryTerm = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    POLICY_COMPLETED -> policyCompleted = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.booleanValue()
                    ROLLED_OVER -> rolledOver = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.booleanValue()
                    TRANSITION_TO -> transitionTo = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.text()
                    StateMetaData.STATE -> {
                        // check null for invalid policy situation
                        state = if (xcp.currentToken() == Token.VALUE_NULL) null else StateMetaData.parse(xcp)
                    }
                    ActionMetaData.ACTION -> {
                        action = if (xcp.currentToken() == Token.VALUE_NULL) null else ActionMetaData.parse(xcp)
                    }
                    StepMetaData.STEP -> {
                        step = if (xcp.currentToken() == Token.VALUE_NULL) null else StepMetaData.parse(xcp)
                    }
                    PolicyRetryInfoMetaData.RETRY_INFO -> {
                        retryInfo = PolicyRetryInfoMetaData.parse(xcp)
                    }
                    INFO -> info = xcp.map()
                    // below line will break when getting metadata for explain or history
                    // else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ManagedIndexMetaData.")
                }
            }

            return ManagedIndexMetaData(
                requireNotNull(index) { "$INDEX is null" },
                requireNotNull(indexUuid) { "$INDEX_UUID is null" },
                requireNotNull(policyID) { "$POLICY_ID is null" },
                policySeqNo,
                policyPrimaryTerm,
                policyCompleted,
                rolledOver,
                transitionTo,
                state,
                action,
                step,
                retryInfo,
                info,
                id,
                seqNo,
                primaryTerm
            )
        }

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parseWithType(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): ManagedIndexMetaData {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            val managedIndexMetaData = parse(xcp, id, seqNo, primaryTerm)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            return managedIndexMetaData
        }

        fun fromMap(map: Map<String, String?>): ManagedIndexMetaData {
            return ManagedIndexMetaData(
                index = requireNotNull(map[INDEX]) { "$INDEX is null" },
                indexUuid = requireNotNull(map[INDEX_UUID]) { "$INDEX_UUID is null" },
                policyID = requireNotNull(map[POLICY_ID]) { "$POLICY_ID is null" },
                policySeqNo = map[POLICY_SEQ_NO]?.toLong(),
                policyPrimaryTerm = map[POLICY_PRIMARY_TERM]?.toLong(),
                policyCompleted = map[POLICY_COMPLETED]?.toBoolean(),
                rolledOver = map[ROLLED_OVER]?.toBoolean(),
                transitionTo = map[TRANSITION_TO],
                stateMetaData = StateMetaData.fromManagedIndexMetaDataMap(map),
                actionMetaData = ActionMetaData.fromManagedIndexMetaDataMap(map),
                stepMetaData = StepMetaData.fromManagedIndexMetaDataMap(map),
                policyRetryInfo = PolicyRetryInfoMetaData.fromManagedIndexMetaDataMap(map),
                info = map[INFO]?.let { XContentHelper.convertToMap(JsonXContent.jsonXContent, it, false) }
            )
        }
    }
}
