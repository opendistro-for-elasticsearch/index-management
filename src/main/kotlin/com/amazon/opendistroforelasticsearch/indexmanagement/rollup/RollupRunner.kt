/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata.Companion.NO_ID
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.script.ScriptService
import java.time.Instant

@Suppress("TooManyFunctions")
object RollupRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var rollupMapperService: RollupMapperService
    private lateinit var rollupIndexer: RollupIndexer
    private lateinit var rollupSearchService: RollupSearchService
    private lateinit var rollupMetadataService: RollupMetadataService

    fun registerClusterService(clusterService: ClusterService): RollupRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): RollupRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): RollupRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): RollupRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): RollupRunner {
        this.settings = settings
        return this
    }

    fun registerMapperService(rollupMapperService: RollupMapperService): RollupRunner {
        this.rollupMapperService = rollupMapperService
        return this
    }

    fun registerIndexer(rollupIndexer: RollupIndexer): RollupRunner {
        this.rollupIndexer = rollupIndexer
        return this
    }

    fun registerSearcher(rollupSearchService: RollupSearchService): RollupRunner {
        this.rollupSearchService = rollupSearchService
        return this
    }

    fun registerMetadataServices(
        rollupMetadataService: RollupMetadataService
    ): RollupRunner {
        this.rollupMetadataService = rollupMetadataService
        return this
    }

    fun registerConsumers(): RollupRunner {
        return this
    }

    @Suppress("ComplexMethod")
    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Rollup) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        launch {
            val metadata: RollupMetadata?
            try {
                metadata = if (job.metadataID != null) {
                    rollupMetadataService.getExistingMetadata(job.metadataID)
                } else null
            } catch (e: Exception) {
                // If the metadata was not able to be retrieved, the exception will be logged and the job run will be a no-op
                logger.error("Failed to get existing rollup metadata [${job.metadataID}]", e)
                return@launch
            }

            // Check if rollup should be processed before acquiring the lock
            // If metadata does not exist, it will either be initialized for the first time or it will be recreated to communicate the failed state
            if (rollupSearchService.shouldProcessRollup(job, metadata)) {
                // Attempt to acquire lock
                val lock: LockModel? = context.lockService.suspendUntil { acquireLock(job, context, it) }
                if (lock == null) {
                    logger.debug("Could not acquire lock for ${job.id}")
                } else {
                    runRollupJob(job, context)
                    // Release lock
                    val released: Boolean = context.lockService.suspendUntil { release(lock, it) }
                    if (!released) {
                        logger.debug("Could not release lock for ${job.id}")
                    }
                }
            }
        }
    }

    // TODO: Clean up runner
    // TODO: Scenario: The rollup job is finished, but I (the user) want to redo it all again
    // TODO: need to get local version of job to see if page size has been changed
    /*
    * TODO situations:
    *  There is a rollup.metadataID and no metadata doc -> create new metadata doc with FAILED status?
    *  There is a rollup.metadataID and doc but theres no target index?
    *        -> index was deleted -> just recreate (but we would have to start over)? Or move to FAILED?
    *  There is a rollup.metadataID and doc but target index is not rollup index?
    *        -> index was deleted and recreated as non rollup -> move to FAILED
    *  There is a rollup.metadataID and doc but theres no job in target index?
    *        -> index was deleted and recreated as rollup -> just recreate (but we would have to start over)? Or move to FAILED?
    * */
    @Suppress("ReturnCount", "NestedBlockDepth", "ComplexMethod", "LongMethod")
    private suspend fun runRollupJob(job: Rollup, context: JobExecutionContext) {
        if (!isJobValid(job)) return

        var metadata = rollupMetadataService.init(job)
        if (metadata.status == RollupMetadata.Status.FAILED) {
            disableJob(job, metadata)
            return
        }

        // If metadata was created for the first time, update job with the id
        var updatableJob = job
        if (updatableJob.metadataID == null && metadata.status == RollupMetadata.Status.INIT) {
            updatableJob = updateRollupJob(updatableJob.copy(metadataID = metadata.id))
        }

        // TODO: before creating target index we also validate the source index exists?
        //  and anything else that would cause this to get delayed right away like invalid fields
        val successful = rollupMapperService.init(updatableJob)
        if (!successful) {
            // TODO: More helpful error messaging
            // TODO: Update to metadata fails
            rollupMetadataService.submitMetadataUpdate(
                metadata = metadata.copy(
                    status = RollupMetadata.Status.FAILED,
                    failureReason = "Failed to initialize the target index",
                    lastUpdatedTime = Instant.now()
                ),
                updating = true
            )
            updateRollupJob(updatableJob.copy(enabled = false, jobEnabledTime = null))
            return
        }

        while (rollupSearchService.shouldProcessRollup(updatableJob, metadata)) {
            do {
                try {
                    val internalComposite = rollupSearchService.executeCompositeSearch(updatableJob, metadata)
                    rollupIndexer.indexRollups(updatableJob, internalComposite)
                    metadata = rollupMetadataService.updateMetadata(updatableJob, metadata, internalComposite)
                    // TODO: Is this still needed now that job is updated earlier in runRollupJob()?
                    //  Job is not being retrieved during the middle of a rollup loop so removing job.metadataID won't be reflected here
                    if (updatableJob.metadataID == null) updatableJob = updateRollupJob(updatableJob.copy(metadataID = metadata.id))
                } catch (e: Exception) {
                    logger.error("Failed to rollup ", e)
                }
            } while (metadata.afterKey != null)
        }

        if (!updatableJob.continuous) {
            if (listOf(RollupMetadata.Status.STOPPED, RollupMetadata.Status.FINISHED, RollupMetadata.Status.FAILED).contains(metadata.status)) {
                updateRollupJob(updatableJob.copy(enabled = false, jobEnabledTime = null))
            }
        }
    }

    // TODO error handling and moving to failed status
    private suspend fun updateRollupJob(job: Rollup): Rollup {
        val req = IndexRollupRequest(rollup = job, refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE)
        val res: IndexRollupResponse = client.suspendUntil { execute(IndexRollupAction.INSTANCE, req, it) }
        // Verify the seqNo/primterm got updated
        return res.rollup
    }

    // TODO: Source index could be a pattern but it's used at runtime so it could match new indices which weren't matched before
    //  which means we always need to validate the source index on every execution?
    // This fn: Should we even proceed to execute this job?
    // Validations could be broken down into:
    // Is this job even allowed to run? i.e. it was in a FAILED state and someone just switched enabled to true, do we actually retry it?
    // Do the job mappings make sense, is it possible to execute this job
    // etc. etc. etc.
    private suspend fun isJobValid(job: Rollup): Boolean {
        // TODO: Handle exceptions
        val metadata = if (job.metadataID != null) {
            rollupMetadataService.getExistingMetadata(job.metadataID)
        } else null

        if (!rollupMapperService.isSourceIndexValid(job.sourceIndex)) {
            setFailedMetadata(job, "Invalid source index")
            return false
        }

        // rollupMetadataService.init() will handle the cases where metadata is null
        if (metadata != null) {
            if (!rollupMapperService.indexExists(job.targetIndex)) {
                // TODO: Handle createRollupTargetIndex fails
                // TODO: Move error reason messages to RollupMetadataService as static strings (or functions for patterns), easier for testing
                rollupMapperService.createRollupTargetIndex(job)
                setFailedMetadata(job, "The target index [${job.targetIndex}] was deleted. Index has been recreated, restart job.")
                return false
            }
            if (!rollupMapperService.isRollupIndex(job.targetIndex)) {
                setFailedMetadata(job, "The target index [${job.targetIndex}] is not a rollup index")
                return false
            }
            if (!rollupMapperService.jobExistsInRollupIndex(job)) {
                setFailedMetadata(job, "The target index [${job.targetIndex}] does not have rollup job information in mappings")
                return false
            }
        }

        return true
    }

    // Sets a failure metadata for the rollup job with the given reason.
    // Can provide an existing metadata to update, if none are provided a new metadata is created
    // and replaces the current one for the job.
    private suspend fun setFailedMetadata(job: Rollup, reason: String, existingMetadata: RollupMetadata? = null) {
        var updatedMetadata: RollupMetadata?
        if (existingMetadata == null) {
            // Create new metadata
            updatedMetadata = RollupMetadata(
                rollupID = job.id,
                status = RollupMetadata.Status.FAILED,
                failureReason = reason,
                lastUpdatedTime = Instant.now()
            )
        } else {
            // Update the given existing metadata
            updatedMetadata = existingMetadata.copy(
                status = RollupMetadata.Status.FAILED,
                failureReason = reason,
                lastUpdatedTime = Instant.now()
            )
        }

        // TODO: Handle exceptions
        updatedMetadata = rollupMetadataService
            .submitMetadataUpdate(updatedMetadata, updatedMetadata.id != NO_ID)

        disableJob(job, updatedMetadata)
    }

    private suspend fun disableJob(job: Rollup, metadata: RollupMetadata) {
        val updatedRollupJob = if (metadata.id != job.metadataID) {
            job.copy(metadataID = metadata.id, enabled = false, jobEnabledTime = null)
        } else {
            job.copy(enabled = false, jobEnabledTime = null)
        }
        // TODO: Handle exceptions
        updateRollupJob(updatedRollupJob)
    }
}
