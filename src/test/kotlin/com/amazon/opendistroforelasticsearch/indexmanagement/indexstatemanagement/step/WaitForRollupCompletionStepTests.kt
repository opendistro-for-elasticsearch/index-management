package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.test.ESTestCase

class WaitForRollupCompletionStepTests : ESTestCase() {

    private val clusterService: ClusterService = mock()

    fun `test wait for rollup when missing rollup id`() {
        val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData
        (WaitForRollupCompletionStep.name, 1, 0, false, 0, null, ActionProperties()), null, null, null)
        val client: Client = mock()
        val step = WaitForRollupCompletionStep(clusterService, client, metadata)

        runBlocking {
            step.execute()
        }

        val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
        assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
    }
}
