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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.ClusterAdminClient
import org.elasticsearch.cluster.SnapshotsInProgress
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.snapshots.Snapshot
import org.elasticsearch.snapshots.SnapshotId
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.transport.RemoteTransportException

class WaitForSnapshotStepTests : ESTestCase() {

    private val clusterService: ClusterService = mock()

    fun `test snapshot missing snapshot name in action properties`() {
        val exception = IllegalArgumentException("not used")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val emptyActionProperties = ActionProperties()
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, emptyActionProperties), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", emptyActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }

        runBlocking {
            val nullActionProperties = null
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, nullActionProperties), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", nullActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot status states`() {
        val snapshotStatus: SnapshotStatus = mock()
        val response: SnapshotsStatusResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotStatus))
        whenever(snapshotStatus.snapshot).doReturn(Snapshot("repo", SnapshotId("snapshot-name", "some_uuid")))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.INIT)
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot in progress message", WaitForSnapshotStep.getSnapshotInProgressMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.STARTED)
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot in progress message", WaitForSnapshotStep.getSnapshotInProgressMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.SUCCESS)
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot completed message", WaitForSnapshotStep.getSuccessMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.ABORTED)
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        whenever(snapshotStatus.state).doReturn(SnapshotsInProgress.State.FAILED)
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot not in response list`() {
        val snapshotStatus: SnapshotStatus = mock()
        val response: SnapshotsStatusResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotStatus))
        whenever(snapshotStatus.snapshot).doReturn(Snapshot("repo", SnapshotId("snapshot-different-name", "some_uuid")))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test snapshot remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val config = SnapshotActionConfig("repo", "snapshot-name", 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(clusterService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(snapshotsStatusResponse: SnapshotsStatusResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (snapshotsStatusResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<SnapshotsStatusResponse>>(1)
                if (snapshotsStatusResponse != null) listener.onResponse(snapshotsStatusResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).snapshotsStatus(any(), any())
        }
    }
}