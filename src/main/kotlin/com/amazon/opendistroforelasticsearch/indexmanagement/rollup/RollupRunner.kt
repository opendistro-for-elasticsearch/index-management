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

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupJobValidationResult
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.incrementStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.mergeStats
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
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
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite
import org.elasticsearch.threadpool.ThreadPool

@Suppress("TooManyFunctions")
object RollupRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
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

    fun registerThreadPool(threadPool: ThreadPool): RollupRunner {
        this.threadPool = threadPool
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
            var metadata: RollupMetadata? = null
            try {
                // Get Metadata does a get request to the config index which the role will not have access to. This is an internal
                // call used by the plugin to populate the metadata itself so do not run this with role's context
                if (job.metadataID != null) {
                    metadata = when (val getMetadataResult = rollupMetadataService.getExistingMetadata(job)) {
                        is MetadataResult.Success -> getMetadataResult.metadata
                        is MetadataResult.NoMetadata -> null
                        is MetadataResult.Failure ->
                            throw RollupMetadataException("Failed to get existing rollup metadata [${job.metadataID}]", getMetadataResult.cause)
                    }
                }
            } catch (e: RollupMetadataException) {
                // If the metadata was not able to be retrieved, the exception will be logged and the job run will be a no-op
                logger.error(e.message, e.cause)
                return@launch
            }

            // Check if rollup should be processed before acquiring the lock
            // If metadata does not exist, it will either be initialized for the first time or it will be recreated to communicate the failed state
            if (rollupSearchService.shouldProcessRollup(job, metadata)) {
                val lock = acquireLockForRollupJob(job, context)
                if (lock == null) {
                    logger.debug("Could not acquire lock for ${job.id}")
                } else {
                    runRollupJob(job, context, lock)
                    releaseLockForRollupJob(context, lock)
                }
            } else if (job.isEnabled) {
                // We are doing this outside of ShouldProcess as schedule job interval can be more frequent than rollup and we want to fail
                // validation as soon as possible
                when (val jobValidity = isJobValid(job)) {
                    is RollupJobValidationResult.Invalid -> {
                        val lock = acquireLockForRollupJob(job, context)
                        if (lock != null) {
                            setFailedMetadataAndDisableJob(job, jobValidity.reason)
                            logger.info("updating metadata service to disable the job [${job.id}]")
                            releaseLockForRollupJob(context, lock)
                        }
                    }
                    else -> {}
                }
            }
        }
    }

    private suspend fun acquireLockForRollupJob(rollupJob: ScheduledJobParameter, context: JobExecutionContext): LockModel? {
        var lock: LockModel? = null
        try {
            // acquireLock will attempt to create the lock index if needed and then read/create a lock. This is purely for internal purposes
            // and should not need the role's context to run
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(
                RollupSettings.DEFAULT_ACQUIRE_LOCK_RETRY_DELAY),
                RollupSettings.DEFAULT_ACQUIRE_LOCK_RETRY_COUNT
            ).retry(logger) {
                lock = context.lockService.suspendUntil { acquireLock(rollupJob, context, it) }
            }
        } catch (e: Exception) {
            logger.error("Error trying to acquireLock", e)
        }

        return lock
    }

    private suspend fun releaseLockForRollupJob(context: JobExecutionContext, lock: LockModel): Boolean {
        var released = false
        try {
            released = context.lockService.suspendUntil { release(lock, it) }
            if (!released) {
                logger.warn("Could not release lock for ${lock.jobId}")
            }
        } catch (e: Exception) {
            logger.error("Failed to release lock", e)
        }
        return released
    }

    // TODO: Clean up runner
    // TODO: Scenario: The rollup job is finished, but I (the user) want to redo it all again
    /*
    * TODO situations:
    *  There is a rollup.metadataID and doc but theres no job in target index?
    *        -> index was deleted and recreated as rollup -> just recreate (but we would have to start over)? Or move to FAILED?
    * */
    @Suppress("ReturnCount", "NestedBlockDepth", "ComplexMethod", "LongMethod", "ThrowsCount")
    private suspend fun runRollupJob(job: Rollup, context: JobExecutionContext, lock: LockModel) {
        var updatableLock = lock
        try {
            when (val jobValidity = isJobValid(job)) {
                is RollupJobValidationResult.Invalid -> {
                    logger.error("Invalid job [${job.id}]: [${jobValidity.reason}]")
                    setFailedMetadataAndDisableJob(job, jobValidity.reason)
                    return
                }
                is RollupJobValidationResult.Failure -> {
                    logger.error("Failed to validate [${job.id}]: [${jobValidity.message}]")
                    setFailedMetadataAndDisableJob(job, jobValidity.message)
                    return
                }
                else -> {}
            }

            // Anything related to creating, reading, and deleting metadata should not require role's context
            var metadata = when (val initMetadataResult = rollupMetadataService.init(job)) {
                is MetadataResult.Success -> initMetadataResult.metadata
                is MetadataResult.NoMetadata -> {
                    logger.info("Init metadata NoMetadata returning early")
                    return
                } // No-op this execution
                is MetadataResult.Failure ->
                    throw RollupMetadataException("Failed to initialize rollup metadata", initMetadataResult.cause)
            }
            if (metadata.status == RollupMetadata.Status.FAILED) {
                logger.info("Metadata status is FAILED, disabling job $metadata")
                disableJob(job, metadata)
                return
            }

            // If metadata was created for the first time, update job with the id
            var updatableJob = job
            if (updatableJob.metadataID == null && metadata.status == RollupMetadata.Status.INIT) {
                when (val updateRollupJobResult = updateRollupJob(updatableJob.copy(metadataID = metadata.id), metadata)) {
                    is RollupJobResult.Success -> updatableJob = updateRollupJobResult.rollup
                    is RollupJobResult.Failure -> {
                        logger.error("Failed to update the rollup job [${updatableJob.id}] with metadata id [${metadata.id}]",
                            updateRollupJobResult.cause)
                        return // Exit runner early
                    }
                }
            }

            when (val result = rollupMapperService.attemptCreateRollupTargetIndex(updatableJob)) {
                is RollupJobValidationResult.Failure -> {
                    setFailedMetadataAndDisableJob(updatableJob, result.message, metadata)
                    return
                }
                is RollupJobValidationResult.Invalid -> {
                    setFailedMetadataAndDisableJob(updatableJob, result.reason, metadata)
                    return
                }
                else -> {}
            }

            while (rollupSearchService.shouldProcessRollup(updatableJob, metadata)) {
                do {
                    try {
                        val rollupResult = when (val rollupSearchResult = rollupSearchService.executeCompositeSearch(updatableJob, metadata)) {
                            is RollupSearchResult.Success -> {
                                val compositeRes: InternalComposite = rollupSearchResult.searchResponse.aggregations.get(updatableJob.id)
                                metadata = metadata.incrementStats(rollupSearchResult.searchResponse, compositeRes)
                                when (val rollupIndexResult = rollupIndexer.indexRollups(updatableJob, compositeRes)) {
                                    is RollupIndexResult.Success -> RollupResult.Success(compositeRes, rollupIndexResult.stats)
                                    is RollupIndexResult.Failure -> RollupResult.Failure(rollupIndexResult.message, rollupIndexResult.cause)
                                }
                            }
                            is RollupSearchResult.Failure -> {
                                RollupResult.Failure(rollupSearchResult.message, rollupSearchResult.cause)
                            }
                        }
                        when (rollupResult) {
                            is RollupResult.Success -> {
                                metadata = rollupMetadataService.updateMetadata(updatableJob,
                                    metadata.mergeStats(rollupResult.stats), rollupResult.internalComposite)
                                updatableJob = client.suspendUntil { listener: ActionListener<GetRollupResponse> ->
                                    execute(GetRollupAction.INSTANCE, GetRollupRequest(updatableJob.id, null, "_local"), listener)
                                }.rollup ?: throw IllegalStateException("Unable to get rollup job")
                            }
                            is RollupResult.Failure -> {
                                rollupMetadataService.updateMetadata(
                                    metadata.copy(status = RollupMetadata.Status.FAILED, failureReason = rollupResult.cause.message)
                                )
                            }
                        }
                        try {
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(
                                RollupSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
                                RollupSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
                            ).retry(logger) {
                                updatableLock = context.lockService.suspendUntil { renewLock(updatableLock, it) }
                            }
                        } catch (e: Exception) {
                            logger.warn("Failed trying to renew lock on $updatableLock", e)
                            // If we fail to renew the lock it doesn't mean we need to perm fail the job, we can just return early
                            // and let the next execution try to process the data from where this one left off
                            releaseLockForRollupJob(context, updatableLock)
                            return
                        }
                    } catch (e: RollupMetadataException) {
                        // Rethrow this exception so it doesn't get consumed here
                        logger.info("RollupMetadataException being thrown", e)
                        throw e
                    } catch (e: Exception) {
                        // TODO: Should update metadata and disable job here instead of allowing the rollup to keep going
                        logger.error("Failed to rollup ", e)
                        releaseLockForRollupJob(context, updatableLock)
                        return
                    }
                } while (metadata.afterKey != null)
            }

            if (!updatableJob.continuous) {
                if (listOf(RollupMetadata.Status.STOPPED, RollupMetadata.Status.FINISHED, RollupMetadata.Status.FAILED).contains(metadata.status)) {
                    disableJob(updatableJob, metadata)
                }
            }

            // If we have been constantly renewing the lock then the seqNo/primaryTerm will have changed
            // and the releaseLock call outside of runRollupJob will fail, so release here with updatableLock
            // and outside just in case we returned early at a different point (attempting to release twice won't hurt)
            releaseLockForRollupJob(context, updatableLock)
        } catch (e: RollupMetadataException) {
            // In most scenarios in the runner, the metadata will be used to communicate the result to the user
            // If change to the metadata itself fails, there is nothing else to relay state change
            // In these cases, the cause of the metadata operation will be logged here and the runner execution will exit
            logger.error(e.message, e.cause)
            releaseLockForRollupJob(context, updatableLock)
        }
    }

    /**
     * Updates a rollup job.
     *
     * Takes in the metadata for the rollup job as well. This metadata should be create/updated
     * before passing it to this method as it will not create/update the metadata during normal conditions.
     *
     * However, in the case that the update to the rollup job fails, the provided metadata will be set to FAILED
     * status and updated to reflect the change.
     */
    private suspend fun updateRollupJob(job: Rollup, metadata: RollupMetadata): RollupJobResult {
        try {
            val req = IndexRollupRequest(rollup = job, refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE)
            val res: IndexRollupResponse = client.suspendUntil { execute(IndexRollupAction.INSTANCE, req, it) }
            // TODO: Verify the seqNo/primterm got updated
            return RollupJobResult.Success(res.rollup)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            // Set metadata to failed since update to rollup job failed
            val errorMessage = "An error occurred when updating rollup job [${job.id}]"
            return when (val setFailedMetadataResult = rollupMetadataService.setFailedMetadata(job, errorMessage, metadata)) {
                is MetadataResult.Success -> RollupJobResult.Failure(errorMessage, e)
                // If the metadata update failed as well, throw an exception to end the runner execution
                is MetadataResult.Failure ->
                    throw RollupMetadataException(setFailedMetadataResult.message, setFailedMetadataResult.cause)
                // Should not get NoMetadata here
                is MetadataResult.NoMetadata ->
                    throw RollupMetadataException("Unexpected state when updating metadata", null)
            }
        }
    }

    // TODO: Source index could be a pattern but it's used at runtime so it could match new indices which weren't matched before
    //  which means we always need to validate the source index on every execution?
    @Suppress("ReturnCount", "ComplexMethod")
    private suspend fun isJobValid(job: Rollup): RollupJobValidationResult {
        var metadata: RollupMetadata? = null
        if (job.metadataID != null) {
            logger.debug("Fetching associated metadata for rollup job [${job.id}]")
            metadata = when (val getMetadataResult = rollupMetadataService.getExistingMetadata(job)) {
                is MetadataResult.Success -> getMetadataResult.metadata
                is MetadataResult.NoMetadata -> null
                is MetadataResult.Failure ->
                    throw RollupMetadataException("Failed to get existing rollup metadata [${job.metadataID}]", getMetadataResult.cause)
            }
        }

        logger.debug("Validating source index [${job.sourceIndex}] for rollup job [${job.id}]")
        when (val sourceIndexValidationResult = rollupMapperService.isSourceIndexValid(job)) {
            is RollupJobValidationResult.Valid -> {} // No action taken when valid
            else -> return sourceIndexValidationResult
        }

        // we validate target index only if there is metadata document in the rollup
        if (metadata != null) {
            logger.debug("Attempting to create/validate target index [${job.targetIndex}] for rollup job [${job.id}]")
            return rollupMapperService.attemptCreateRollupTargetIndex(job)
        }

        return RollupJobValidationResult.Valid
    }

    /**
     * Sets a failed metadata (updating an existing metadata if provided, otherwise creating a new one) and disables the job.
     *
     * Returns true if disabling the job was successful. If any metadata operations fail along the way, RollupMetadataException
     * is thrown to be caught by the runner.
     */
    private suspend fun setFailedMetadataAndDisableJob(job: Rollup, reason: String, existingMetadata: RollupMetadata? = null): Boolean {
        val updatedMetadata = when (val setFailedMetadataResult = rollupMetadataService.setFailedMetadata(job, reason, existingMetadata)) {
            is MetadataResult.Success -> setFailedMetadataResult.metadata
            is MetadataResult.Failure ->
                throw RollupMetadataException(setFailedMetadataResult.message, setFailedMetadataResult.cause)
            // Should not get NoMetadata here
            is MetadataResult.NoMetadata ->
                throw RollupMetadataException("Unexpected state when setting failed metadata", null)
        }

        return disableJob(job, updatedMetadata)
    }

    /**
     * Disables a given job. Also takes in an existing metadata to replace the metadataID of the job
     * if this was a newly created metadata.
     *
     * This method will return true or false based on whether or not updating the job was successful. When
     * calling this method, it can be assumed that when the response is false, the failed metadata was updated
     * to relay the failure. If the metadata update failed, a RollupMetadataException would be thrown which gets
     * caught in the runner so that case does not need to be explicitly handled by the caller of the method.
     *
     * Note: Set metadata to failed and ensure it's updated before passing it to this method because it
     * will not update the metadata (unless updateRollupJob job fails).
     */
    private suspend fun disableJob(job: Rollup, metadata: RollupMetadata): Boolean {
        val updatedRollupJob = if (metadata.id != job.metadataID) {
            job.copy(metadataID = metadata.id, enabled = false, jobEnabledTime = null)
        } else {
            job.copy(enabled = false, jobEnabledTime = null)
        }

        return when (val updateRollupJobResult = updateRollupJob(updatedRollupJob, metadata)) {
            is RollupJobResult.Success -> true
            is RollupJobResult.Failure -> {
                logger.error("Failed to disable rollup job [${job.id}]", updateRollupJobResult.cause)
                false
            }
        }
    }
}

sealed class RollupJobResult {
    data class Success(val rollup: Rollup) : RollupJobResult()
    data class Failure(val message: String = "An error occurred for rollup job", val cause: Exception) : RollupJobResult()
}

sealed class RollupResult {
    data class Success(val internalComposite: InternalComposite, val stats: RollupStats) : RollupResult()
    data class Failure(val message: String = "An error occurred while rolling up", val cause: Exception) : RollupResult()
}
