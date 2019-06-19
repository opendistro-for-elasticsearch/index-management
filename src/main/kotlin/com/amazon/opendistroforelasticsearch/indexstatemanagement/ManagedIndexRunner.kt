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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.getManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.index.Index
import org.elasticsearch.threadpool.ThreadPool

object ManagedIndexRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var threadPool: ThreadPool

    fun registerClusterService(clusterService: ClusterService): ManagedIndexRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): ManagedIndexRunner {
        this.client = client
        return this
    }

    // TODO we may not need ThreadPool if we use Kotlin Coroutine. This is just temporary implementation for simplicity.
    fun registerThreadPool(threadPool: ThreadPool): ManagedIndexRunner {
        this.threadPool = threadPool
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        logger.info("runJob for ${job.name}")

        // TODO Use Kotlin Coroutine. This just temporary implementation.
        val runnable = Runnable {
            managedIndexRun(job, context)
        }

        threadPool.generic().submit(runnable)
    }

    private fun managedIndexRun(job: ScheduledJobParameter, context: JobExecutionContext) {
        context.expectedExecutionTime
        if (job !is ManagedIndexConfig) {
            logger.error("Run job must be of type ManagedIndexConfig")
            // TODO maybe throw an exception?
            return
        }

        // TODO This runner implementation is just temporary. This is example to show how to read and write the IndexMetadata.
        val index = Index(job.index, job.indexUuid)
        val indexMetaData = clusterService.state().metaData().index(index)

        val existingManagedMetadata = indexMetaData.getManagedIndexMetaData()

        if (existingManagedMetadata != null) {
            logger.info("Start from where we left off. $existingManagedMetadata")
        } else {
            logger.info("Start from default state.")
        }

        // Update ManagedIndexMetadata
        val updateMetadata = ManagedIndexMetaData(
            indexMetaData.index.name,
            indexMetaData.index.uuid,
            "${indexMetaData.index.name}_POLICY_NAME",
            "${indexMetaData.index.name}_POLICY_VERSION",
            "${indexMetaData.index.name}_STATE",
            "${indexMetaData.index.name}_STATE_START_TIME",
            "${indexMetaData.index.name}_ACTION_INDEX",
            "${indexMetaData.index.name}_ACTION",
            "${indexMetaData.index.name}_ACTION_START_TIME",
            "${indexMetaData.index.name}_STEP",
            "${indexMetaData.index.name}_STEP_START_TIME",
            "${indexMetaData.index.name}_FAILED_STEP"
        )

        val request = UpdateManagedIndexMetaDataRequest(index, updateMetadata)
        client.execute(UpdateManagedIndexMetaDataAction, request).actionGet()
    }
}
