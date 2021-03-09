/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMetadataException
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.util.acquireLockForScheduledJob
import com.amazon.opendistroforelasticsearch.indexmanagement.util.releaseLockForScheduledJob
import com.amazon.opendistroforelasticsearch.indexmanagement.util.renewLockForScheduledJob
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import java.time.Instant
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.NamedXContentRegistry

object TransformRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TransformRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var esClient: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings
    private lateinit var transformMetadataService: TransformMetadataService
    private lateinit var transformSearchService: TransformSearchService
    private lateinit var transformIndexer: TransformIndexer

    fun initialize(client: Client, clusterService: ClusterService, xContentRegistry: NamedXContentRegistry, settings: Settings): TransformRunner {
        this.clusterService = clusterService
        this.esClient = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.transformSearchService = TransformSearchService(settings, clusterService, client)
        this.transformMetadataService = TransformMetadataService(client, xContentRegistry)
        this.transformIndexer = TransformIndexer(settings, clusterService, client)
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Transform) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}]")
        }

        launch {
            try {
                val metadata = transformMetadataService.getMetadata(job)
                if (shouldProcessTransform(job, metadata)) {
                    executeJob(job, metadata, context)
                }
            } catch (e: RollupMetadataException) {
                logger.error("Failed to run job [${job.id}] because ${e.localizedMessage}")
                return@launch
            }
        }
    }

    private suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var currentMetadata = metadata
        var updatableTransform = transform
        val backoffPolicy = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
            TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
        )
        var lock = acquireLockForScheduledJob(updatableTransform, context, backoffPolicy)
        do {
            if (lock == null) {
                logger.warn("Cannot acquire lock for transform job ${updatableTransform.id}")
                // If we fail to get the lock we won't fail the job, instead we return early
                return
            } else {
                // TODO: Should we check if the transform job is valid every execute (?)
                // TODO: Check things about executing the job further or not
                try {
                    val (meta, docsToIndex) = transformSearchService.executeCompositeSearch(transform, metadata)
                    val (stats, afterKey) = meta
                    val indexTimeInMillis = transformIndexer.index(docsToIndex)
                    val updatedStats = stats.copy(indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis)
                    currentMetadata = currentMetadata.mergeStats(updatedStats).copy(
                        afterKey = afterKey,
                        lastUpdatedAt = Instant.now(),
                        status = if (afterKey == null) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
                    )
                    if (transform.metadataId == null) {
                        updatableTransform = transform.copy(
                            metadataId = metadata.id,
                            updatedAt = Instant.now()
                        )
                        updatableTransform = updateTransform(updatableTransform)
                    }
                    transformMetadataService.writeMetadata(metadata, true)
                } catch (e: Exception) {
                    logger.error(e.localizedMessage)
                    currentMetadata = metadata.copy(
                        lastUpdatedAt = Instant.now(),
                        status = TransformMetadata.Status.FAILED,
                        failureReason = e.localizedMessage
                    )
                    transformMetadataService.writeMetadata(currentMetadata, true)
                    releaseLockForScheduledJob(context, lock)
                }

                // we attempt to renew lock for every loop of transform
                val renewedLock = renewLockForScheduledJob(context, lock, backoffPolicy)
                if (renewedLock == null) {
                    releaseLockForScheduledJob(context, lock)
                }
                lock = renewedLock
            }
        } while (currentMetadata.afterKey != null)
    }

    private suspend fun updateTransform(transform: Transform): Transform {
        try {
            val request = IndexTransformRequest(transform = transform, refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE)
            val response: IndexTransformResponse = esClient.suspendUntil { execute(IndexTransformAction.INSTANCE, request, it) }
            return transform.copy(
                seqNo = response.seqNo,
                primaryTerm = response.primaryTerm
            )
        } catch (e: Exception) {
            throw Exception("Failed to update the transform job because of [${e.localizedMessage}]")
        }
    }

    private fun shouldProcessTransform(transform: Transform, metadata: TransformMetadata): Boolean {
        if (!transform.enabled ||
            listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FAILED, TransformMetadata.Status.FINISHED).contains(metadata.status)) {
            return false
        }

        return true
    }
}
