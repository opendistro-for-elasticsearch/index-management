package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class WaitForRollupCompletionStep(
    val clusterService: ClusterService,
    val client: Client,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("wait_for_rollup_completion", managedIndexMetaData) {

    override fun isIdempotent() = true

    override suspend fun execute(): WaitForRollupCompletionStep {
        TODO("Not yet implemented")
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        TODO("Not yet implemented")
    }
}
