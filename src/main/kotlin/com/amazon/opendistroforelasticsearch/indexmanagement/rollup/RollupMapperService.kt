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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupJobValidationResult
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Histogram
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion._META
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.getFieldFromMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.MappingMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.RemoteTransportException

// TODO: Validation of fields across source and target indices overwriting existing rollup data
//  and type validation using mappings from source index
// TODO: Wrap client calls in retry for transient failures
@Suppress("TooManyFunctions")
class RollupMapperService(
    val client: Client,
    val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) {

    private val logger = LogManager.getLogger(javaClass)

    // If the index already exists we need to verify it's a rollup index,
    // confirm it does not conflict with existing jobs and is a valid job
    @Suppress("ReturnCount")
    private suspend fun validateAndAttemptToUpdateTargetIndex(rollup: Rollup): RollupJobValidationResult {
        if (!isRollupIndex(rollup.targetIndex)) {
            return RollupJobValidationResult.Invalid("Target index [${rollup.targetIndex}] is a non rollup index")
        }

        return when (val jobExistsResult = jobExistsInRollupIndex(rollup)) {
            is RollupJobValidationResult.Valid -> jobExistsResult
            is RollupJobValidationResult.Invalid -> updateRollupIndexMappings(rollup)
            is RollupJobValidationResult.Failure -> jobExistsResult
        }
    }

    // This creates the target index if it doesn't already else validate the target index is rollup index
    // If the target index mappings doesn't contain rollup job attempts to update the mappings.
    // TODO: error handling
    @Suppress("ReturnCount")
    suspend fun attemptCreateRollupTargetIndex(job: Rollup): RollupJobValidationResult {
        if (indexExists(job.targetIndex)) {
            return validateAndAttemptToUpdateTargetIndex(job)
        } else {
            val errorMessage = "Failed to create target index [${job.targetIndex}]"
            return try {
                val response = createTargetIndex(job)
                if (response.isAcknowledged) {
                    updateRollupIndexMappings(job)
                } else {
                    RollupJobValidationResult.Failure(errorMessage)
                }
            } catch (e: RemoteTransportException) {
                val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                logger.error(errorMessage, unwrappedException)
                RollupJobValidationResult.Failure(errorMessage, unwrappedException)
            } catch (e: Exception) {
                logger.error("$errorMessage because ", e)
                RollupJobValidationResult.Failure(errorMessage, e)
            }
        }
    }

    private suspend fun createTargetIndex(job: Rollup): CreateIndexResponse {
        val request = CreateIndexRequest(job.targetIndex)
            .settings(Settings.builder().put(RollupSettings.ROLLUP_INDEX.key, true).build())
            .mapping(_DOC, IndexManagementIndices.rollupTargetMappings, XContentType.JSON)
        // TODO: Perhaps we can do better than this for mappings... as it'll be dynamic for rest
        //  Can we read in the actual mappings from the source index and use that?
        //  Can it have issues with metrics? i.e. an int mapping with 3, 5, 6 added up and divided by 3 for avg is 14/3 = 4.6666
        //  What happens if the first indexing is an integer, i.e. 3 + 3 + 3 = 9/3 = 3 and it saves it as int
        //  and then the next is float and it fails or rounds it up? Does elasticsearch dynamically resolve to int?
        return client.admin().indices().suspendUntil { create(request, it) }
    }

    // Source index can be a pattern so will need to resolve the index to concrete indices and check:
    // 1. If there are any indices resolving to the given source index
    // 2. That each concrete index is valid (in terms of mappings, etc.)
    @Suppress("ReturnCount")
    suspend fun isSourceIndexValid(rollup: Rollup): RollupJobValidationResult {
        // TODO: Add some entry in metadata that will store index -> indexUUID for validated indices
        //  That way, we only need to validate indices that aren't in the metadata (covers cases where new index with same name was made)
        // Allow no indices, open and closed
        // Rolling up on closed indices will not be caught here
        val concreteIndices =
            indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), rollup.sourceIndex)
        if (concreteIndices.isEmpty()) return RollupJobValidationResult.Invalid("No indices found for [${rollup.sourceIndex}]")

        // Validate mappings for each concrete index resolved from the rollup source index
        concreteIndices.forEach { index ->
            when (val sourceIndexMappingResult = isSourceIndexMappingsValid(index, rollup)) {
                is RollupJobValidationResult.Valid -> {} // no-op if valid
                is RollupJobValidationResult.Invalid -> return sourceIndexMappingResult
                is RollupJobValidationResult.Failure -> return sourceIndexMappingResult
            }
        }

        return RollupJobValidationResult.Valid
    }

    @Suppress("ReturnCount", "ComplexMethod")
    private suspend fun isSourceIndexMappingsValid(index: String, rollup: Rollup): RollupJobValidationResult {
        try {
            val res = when (val getMappingsResult = getMappings(index)) {
                is GetMappingsResult.Success -> getMappingsResult.response
                is GetMappingsResult.Failure ->
                    return RollupJobValidationResult.Failure(getMappingsResult.message, getMappingsResult.cause)
            }

            val indexMapping: MappingMetadata = res.mappings[index][_DOC]
            val indexMappingSource = indexMapping.sourceAsMap

            val issues = mutableSetOf<String>()
            // Validate source fields in dimensions
            rollup.dimensions.forEach { dimension ->
                if (!isFieldInMappings(dimension.sourceField, indexMappingSource))
                    issues.add("missing field ${dimension.sourceField}")

                when (dimension) {
                    is DateHistogram -> {
                        // TODO: Validate if field is date type: date, date_nanos?
                    }
                    is Histogram -> {
                        // TODO: Validate field types for histograms
                    }
                    is Terms -> {
                        // TODO: Validate field types for terms
                    }
                }
            }

            // Validate source fields in metrics
            rollup.metrics.forEach { metric ->
                if (!isFieldInMappings(metric.sourceField, indexMappingSource))
                    issues.add("missing field ${metric.sourceField}")

                // TODO: Validate field type for metrics,
                //  are all Numeric field types valid?
            }

            return if (issues.isEmpty()) {
                RollupJobValidationResult.Valid
            } else {
                RollupJobValidationResult.Invalid("Invalid mappings for index [$index] because $issues")
            }
        } catch (e: Exception) {
            return RollupJobValidationResult.Failure("Failed to validate the source index mappings", e)
        }
    }

    /**
     * Checks to see if the given field name is in the mappings map.
     *
     * The field name can be a path in the format "field1.field2...fieldn" so each field
     * will be checked in the map to get the next level until all subfields are checked for,
     * in which case true is returned. If at any point any of the fields is not in the map, false is returned.
     */
    private fun isFieldInMappings(fieldName: String, mappings: Map<*, *>): Boolean {
        val field = getFieldFromMappings(fieldName, mappings)
        return field != null
    }

    private suspend fun jobExistsInRollupIndex(rollup: Rollup): RollupJobValidationResult {
        val res = when (val getMappingsResult = getMappings(rollup.targetIndex)) {
            is GetMappingsResult.Success -> getMappingsResult.response
            is GetMappingsResult.Failure ->
                return RollupJobValidationResult.Failure(getMappingsResult.message, getMappingsResult.cause)
        }

        val indexMapping: MappingMetadata = res.mappings[rollup.targetIndex][_DOC]

        return if (((indexMapping.sourceAsMap?.get(_META) as Map<*, *>?)?.get(ROLLUPS) as Map<*, *>?)?.containsKey(rollup.id) == true) {
            RollupJobValidationResult.Valid
        } else {
            RollupJobValidationResult.Invalid("Rollup job [${rollup.id}] does not exist in rollup index [${rollup.targetIndex}]")
        }
    }

    @Suppress("ReturnCount")
    private suspend fun getMappings(index: String): GetMappingsResult {
        val errorMessage = "Failed to get mappings for index [$index]"
        try {
            val req = GetMappingsRequest().indices(index)
            val res: GetMappingsResponse? = client.admin().indices().suspendUntil { getMappings(req, it) }
            return if (res == null) {
                GetMappingsResult.Failure(cause = IllegalStateException("GetMappingsResponse for index [$index] was null"))
            } else {
                GetMappingsResult.Success(res)
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            return GetMappingsResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            return GetMappingsResult.Failure(errorMessage, e)
        }
    }

    fun isRollupIndex(index: String): Boolean = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)

    fun indexExists(index: String): Boolean = clusterService.state().routingTable.hasIndex(index)

    // TODO: error handling - can RemoteTransportException happen here?
    // TODO: The use of the master transport action UpdateRollupMappingAction will prevent
    //   overwriting an existing rollup job _meta by checking for the job id
    //   but there is still a race condition if two jobs are added at the same time for the
    //   same target index. There is a small time window after get mapping and put mappings
    //   where they can both get the same mapping state and only add their own job, meaning one
    //   of the jobs won't be added to the target index _meta
    @Suppress("BlockingMethodInNonBlockingContext", "ReturnCount")
    private suspend fun updateRollupIndexMappings(rollup: Rollup): RollupJobValidationResult {
        val errorMessage = "Failed to update mappings of target index [${rollup.targetIndex}] with rollup job"
        try {
            val response = withContext(Dispatchers.IO) {
                val resp: AcknowledgedResponse = client.suspendUntil {
                    execute(UpdateRollupMappingAction.INSTANCE, UpdateRollupMappingRequest(rollup), it)
                }
                resp.isAcknowledged
            }

            if (!response) {
                // TODO: when this happens is it failure or invalid?
                logger.error("$errorMessage with no exception")
                return RollupJobValidationResult.Failure(errorMessage)
            }
            return RollupJobValidationResult.Valid
        } catch (e: Exception) {
            logger.error("$errorMessage because ", e)
            return RollupJobValidationResult.Failure(errorMessage, e)
        }
    }

    companion object {
        const val ROLLUPS = "rollups"
    }

    sealed class GetMappingsResult {
        data class Success(val response: GetMappingsResponse) : GetMappingsResult()
        data class Failure(val message: String = "An error occurred when getting mappings", val cause: Exception) : GetMappingsResult()
    }
}
