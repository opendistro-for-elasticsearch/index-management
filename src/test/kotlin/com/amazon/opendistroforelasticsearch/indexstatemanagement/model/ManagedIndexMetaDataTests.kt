package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.PolicyRetryInfoMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StateMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.StepMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.step.Step
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput
import org.elasticsearch.test.ESTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class ManagedIndexMetaDataTests : ESTestCase() {

    fun `test basic`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully initialized policy: close_policy")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test action`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = ActionMetaData("close", 4321, 0, false, 0, 0, null),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully closed index")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test action property`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = null,
            transitionTo = null,
            stateMetaData = StateMetaData("close-index", 1234),
            actionMetaData = ActionMetaData("close", 4321, 0, false, 0, 0, ActionProperties(null, 3)),
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully closed index")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    fun `test step`() {
        val expectedManagedIndexMetaData = ManagedIndexMetaData(
            index = "movies",
            indexUuid = "ahPcR4fNRrSe-Q7czV3VPQ",
            policyID = "close_policy",
            policySeqNo = 0,
            policyPrimaryTerm = 1,
            policyCompleted = null,
            rolledOver = false,
            transitionTo = null,
            stateMetaData = StateMetaData("rollover-index", 1234),
            actionMetaData = ActionMetaData("rollover", 4321, 0, false, 0, 0, null),
            stepMetaData = StepMetaData("attempt_rollover", 6789, Step.StepStatus.FAILED),
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "There is no valid rollover_alias=null set on movies")
        )

        roundTripManagedIndexMetaData(expectedManagedIndexMetaData)
    }

    private fun roundTripManagedIndexMetaData(expectedManagedIndexMetaData: ManagedIndexMetaData) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedManagedIndexMetaData.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualManagedIndexMetaData = ManagedIndexMetaData.fromStreamInput(input)
        assertEquals(expectedManagedIndexMetaData, actualManagedIndexMetaData)
    }
}
