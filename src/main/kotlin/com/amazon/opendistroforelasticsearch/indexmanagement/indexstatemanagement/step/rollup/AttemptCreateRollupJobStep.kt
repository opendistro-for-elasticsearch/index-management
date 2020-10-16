package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.Step
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService

class AttemptCreateRollupJobStep(
    val clusterService: ClusterService,
    val client: Client,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_create_rollup", managedIndexMetaData) {

    override fun isIdempotent() = false

    override suspend fun execute(): Step {
        TODO("Not yet implemented")
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        TODO("Not yet implemented")
    }
}
