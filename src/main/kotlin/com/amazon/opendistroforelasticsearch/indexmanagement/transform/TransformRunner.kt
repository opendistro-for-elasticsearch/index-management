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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
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
    private lateinit var transformValidator: TransformValidator

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings,
        indexNameExpressionResolver: IndexNameExpressionResolver
    ): TransformRunner {
        this.clusterService = clusterService
        this.esClient = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.transformSearchService = TransformSearchService(settings, clusterService, client)
        this.transformMetadataService = TransformMetadataService(client, xContentRegistry)
        this.transformIndexer = TransformIndexer(settings, clusterService, client)
        this.transformValidator = TransformValidator(indexNameExpressionResolver, clusterService, client)
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Transform) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}]")
        }

        launch {
            try {
                if (job.enabled) {
                    val metadata = transformMetadataService.getMetadata(job)
                    var transform = job
                    if (job.metadataId == null) {
                        transform = updateTransform(job.copy(metadataId = metadata.id))
                    }
                    executeJob(transform, metadata, context)
                }
            } catch (e: Exception) {
                logger.error("Failed to run job [${job.id}] because ${e.localizedMessage}", e)
                return@launch
            }
        }
    }

    // TODO: Add circuit breaker checks - [cluster healthy, utilization within limit]
    @Suppress("NestedBlockDepth", "ComplexMethod")
    private suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var currentMetadata = metadata
        val backoffPolicy = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
            TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
        )
        var lock = acquireLockForScheduledJob(transform, context, backoffPolicy)
        try {
            do {
                when {
                    lock == null -> {
                        logger.warn("Cannot acquire lock for transform job ${transform.id}")
                        // If we fail to get the lock we won't fail the job, instead we return early
                        return
                    }
                    listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FINISHED).contains(metadata.status) -> {
                        logger.warn("Transform job ${transform.id} is in ${metadata.status} status. Skipping execution")
                        return
                    }
                    else -> {
                        currentMetadata = executeJobIteration(transform, currentMetadata)
                        // we attempt to renew lock for every loop of transform
                        val renewedLock = renewLockForScheduledJob(context, lock, backoffPolicy)
                        if (renewedLock == null) {
                            releaseLockForScheduledJob(context, lock)
                        }
                        lock = renewedLock
                    }
                }
            } while (currentMetadata.afterKey != null)
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            lock?.let {
                transformMetadataService.writeMetadata(currentMetadata, true)
                logger.info("Disabling the transform job ${transform.id}")
                updateTransform(transform.copy(enabled = false, enabledAt = null))
                releaseLockForScheduledJob(context, it)
            }
        }
    }

    private suspend fun executeJobIteration(transform: Transform, metadata: TransformMetadata): TransformMetadata {
        val validationResult = transformValidator.validate(transform)
        if (validationResult.isValid) {
            val transformSearchResult = transformSearchService.executeCompositeSearch(transform, metadata.afterKey)
            val indexTimeInMillis = transformIndexer.index(transformSearchResult.docsToIndex)
            val afterKey = transformSearchResult.afterKey
            val stats = transformSearchResult.stats
            val updatedStats = stats.copy(
                indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis, documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
            )
            val updatedMetadata = metadata.mergeStats(updatedStats).copy(
                afterKey = afterKey,
                lastUpdatedAt = Instant.now(),
                status = if (afterKey == null) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
            )
            return transformMetadataService.writeMetadata(updatedMetadata, true)
        } else {
            val failureMessage = "Failed validation - ${validationResult.issues}"
            val updatedMetadata = metadata.copy(status = TransformMetadata.Status.FAILED, failureReason = failureMessage)
            return transformMetadataService.writeMetadata(updatedMetadata, true)
        }
    }

    private suspend fun updateTransform(transform: Transform): Transform {
        val request = IndexTransformRequest(
            transform = transform.copy(updatedAt = Instant.now()),
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        )
        val response: IndexTransformResponse = esClient.suspendUntil { execute(IndexTransformAction.INSTANCE, request, it) }
        return transform.copy(
            seqNo = response.seqNo,
            primaryTerm = response.primaryTerm
        )
    }
}
