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
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import java.lang.IllegalStateException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.transport.RemoteTransportException

class TransformValidator(
    private val indexNameExpressionResolver: IndexNameExpressionResolver,
    private val clusterService: ClusterService,
    private val esClient: Client
) {

    // TODO: When FGAC is supported in transform should check the user has the correct permissions
    suspend fun validate(transform: Transform): TransformValidationResult {
        val errorMessage = "Failed to validate the transform job"
        try {
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), transform.sourceIndex)
            if (concreteIndices.isEmpty()) return TransformValidationResult(false, listOf("No specified source index exist in the cluster"))

            val issues = mutableListOf<String>()
            concreteIndices.forEach { index -> issues.addAll(validateIndex(index, transform)) }

            return TransformValidationResult(issues.isEmpty(), issues)
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformValidationException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            throw TransformValidationException(errorMessage, e)
        }
    }

    private suspend fun validateIndex(index: String, transform: Transform): List<String> {
        val request = GetMappingsRequest().indices(index)
        val result: GetMappingsResponse =
            esClient.admin().indices().suspendUntil { getMappings(request, it) }?:
            throw IllegalStateException("GetMappingResponse for [$index] was null")
        val mappings = result.mappings[index][_DOC].sourceAsMap

        val issues = mutableListOf<String>()
        transform.groups.forEach { group ->
            if (!group.canBeRealizedInMappings(mappings)) {
                issues.add("Cannot find a field [${group.sourceField}] that can be grouped as [${group.type.type}] in [$index]")
            }
        }
        return issues
    }
}
