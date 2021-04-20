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
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.exceptions.TransformValidationException
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformValidationResult
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings
import java.lang.IllegalStateException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.monitor.jvm.JvmService
import org.elasticsearch.transport.RemoteTransportException

class TransformValidator(
    private val indexNameExpressionResolver: IndexNameExpressionResolver,
    private val clusterService: ClusterService,
    private val esClient: Client,
    val settings: Settings,
    private val jvmService: JvmService
) {

    @Volatile private var circuitBreakerEnabled = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED.get(settings)
    @Volatile private var circuitBreakerJvmThreshold = TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED) {
            circuitBreakerEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD) {
            circuitBreakerJvmThreshold = it
        }
    }
    /**
     * // TODO: When FGAC is supported in transform should check the user has the correct permissions
     * Validates the provided transform. Validation checks include the following:
     * 1. Source index/indices defined in transform exist
     * 2. Groupings defined in transform can be realized using source index/indices
     */
    suspend fun validate(transform: Transform): TransformValidationResult {
        val errorMessage = "Failed to validate the transform job"
        try {
            val issues = mutableListOf<String>()
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), transform.sourceIndex)
            if (concreteIndices.isEmpty()) return TransformValidationResult(false, listOf("No specified source index exist in the cluster"))

            val request = ClusterHealthRequest()
                .indices(*concreteIndices)
                .waitForYellowStatus()
            val response: ClusterHealthResponse = esClient.suspendUntil { execute(ClusterHealthAction.INSTANCE, request, it) }
            if (response.isTimedOut) {
                issues.add("Cannot determine that the requested source indices are healthy")
                return TransformValidationResult(issues.isEmpty(), issues)
            }

            if (circuitBreakerEnabled && jvmService.stats().mem.heapUsedPercent > circuitBreakerJvmThreshold) {
                issues.add("The cluster is breaching the jvm usage threshold [$circuitBreakerJvmThreshold], cannot execute the transform")
                return TransformValidationResult(issues.isEmpty(), issues)
            }
            concreteIndices.forEach { index -> issues.addAll(validateIndex(index, transform)) }

            return TransformValidationResult(issues.isEmpty(), issues)
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformValidationException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            throw TransformValidationException(errorMessage, e)
        }
    }

    /**
     * Internal method to validate grouping defined inside transform can be realized with the index provided.
     * 1. Checks that each field mentioned in transform groupings exist in source index
     * 2. Checks that type of field in index can be grouped as requested
     */
    private suspend fun validateIndex(index: String, transform: Transform): List<String> {
        val request = GetMappingsRequest().indices(index)
        val issues = mutableListOf<String>()
        val result: GetMappingsResponse =
            esClient.admin().indices().suspendUntil { getMappings(request, it) } ?: throw IllegalStateException("GetMappingResponse for [$index] " +
                                                                                                                    "was null")
        val indexTypeMappings = result.mappings[index]
        if (indexTypeMappings.isEmpty) {
            issues.add("Source index [$index] mappings are empty, cannot validate the job.")
            return issues
        }

        // Starting from 6.0.0 an index can only have one mapping type, but mapping type is still part of the APIs in 7.x, allowing users to
        // set a custom mapping type. As a result using first mapping type found instead of _DOC mapping type to validate
        val indexMappingSource = indexTypeMappings.first().value.sourceAsMap

        transform.groups.forEach { group ->
            if (!group.canBeRealizedInMappings(indexMappingSource)) {
                issues.add("Cannot find a field [${group.sourceField}] that can be grouped as [${group.type.type}] in [$index]")
            }
        }
        return issues
    }
}
