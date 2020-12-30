package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.ClusterAdminClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException
import org.elasticsearch.test.ESTestCase
import org.elasticsearch.transport.RemoteTransportException

class AttemptSnapshotStepTests : ESTestCase() {

    private val clusterService: ClusterService = mock()
    private val config = SnapshotActionConfig("repo", "snapshot-name", 0)
    private val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(AttemptSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)

    private var settings: Map<String, Any> = mapOf(SNAPSHOT_DENY_LIST.key to emptyList<String>())

    fun `test snapshot response when block`() {
        val response: CreateSnapshotResponse = mock()
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        whenever(response.status()).doReturn(RestStatus.ACCEPTED)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }

        whenever(response.status()).doReturn(RestStatus.OK)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }

        whenever(response.status()).doReturn(RestStatus.INTERNAL_SERVER_ERROR)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test snapshot exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test snapshot concurrent snapshot exception`() {
        val exception = ConcurrentSnapshotExecutionException("repo", "other-snapshot", "concurrent snapshot in progress")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get failed concurrent message", AttemptSnapshotStep.getFailedConcurrentSnapshotMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot remote transport concurrent exception`() {
        val exception = RemoteTransportException("rte", ConcurrentSnapshotExecutionException("repo", "other-snapshot", "concurrent snapshot in progress"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get failed concurrent message", AttemptSnapshotStep.getFailedConcurrentSnapshotMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot remote transport normal exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("some error"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, client, config, metadata, settings)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "some error", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(createSnapshotRequest: CreateSnapshotResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (createSnapshotRequest != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<CreateSnapshotResponse>>(1)
                if (createSnapshotRequest != null) listener.onResponse(createSnapshotRequest)
                else listener.onFailure(exception)
            }.whenever(this.mock).createSnapshot(any(), any())
        }
    }
}