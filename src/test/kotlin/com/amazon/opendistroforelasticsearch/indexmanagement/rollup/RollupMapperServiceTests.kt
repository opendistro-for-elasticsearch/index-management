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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupJobValidationResult
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.anyVararg
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.MappingMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.collect.ImmutableOpenMap
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import java.time.Instant

class RollupMapperServiceTests : ESTestCase() {

    fun `test source index validation`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(randomDateHistogram().copy(
            sourceField = "order_date"
        ))
        val metrics = listOf(randomRollupMetrics().copy(
            sourceField = "total_quantity"
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(getAdminClient(getIndicesAdminClient(
            getMappingsResponse = getKibanaSampleDataMappingResponse(sourceIndex),
            getMappingsException = null
        )))
        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation with nested field`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(randomDateHistogram().copy(
            sourceField = "order_date"
        ))
        val metrics = listOf(randomRollupMetrics().copy(
            sourceField = "products.quantity"
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(getAdminClient(getIndicesAdminClient(
            getMappingsResponse = getKibanaSampleDataMappingResponse(sourceIndex),
            getMappingsException = null
        )))
        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation when field is not in mapping`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(randomDateHistogram().copy(
            sourceField = "nonexistent_field"
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = emptyList()
        )

        val client = getClient(getAdminClient(getIndicesAdminClient(
            getMappingsResponse = getKibanaSampleDataMappingResponse(sourceIndex),
            getMappingsException = null
        )))
        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Invalid) { "Source index validation returned unexpected results" }

            assertEquals("Invalid mappings for index [$sourceIndex] because [missing field nonexistent_field]", sourceIndexValidationResult.reason)
        }
    }

    // TODO: Test scenarios:
    //  - Source index validation when GetMappings throws an exception
    //  - Source index validation for correct/incorrect field types for different dimensions/metrics when that is added

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(
        getMappingsResponse: GetMappingsResponse?,
        getMappingsException: Exception?
    ): IndicesAdminClient {
        assertTrue("Must provide either a getMappingsResponse or getMappingsException",
            (getMappingsResponse != null).xor(getMappingsException != null))

        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetMappingsResponse>>(1)
                if (getMappingsResponse != null) listener.onResponse(getMappingsResponse)
                else listener.onFailure(getMappingsException)
            }.whenever(this.mock).getMappings(any(), any())
        }
    }

    private fun getClusterService(): ClusterService = mock { on { state() } doReturn mock() }

    // Returns a mocked IndexNameExpressionResolver which returns the provided list of concrete indices
    private fun getIndexNameExpressionResolver(concreteIndices: List<String>): IndexNameExpressionResolver =
        mock { on { concreteIndexNames(any(), any(), anyVararg<String>()) } doReturn concreteIndices.toTypedArray() }

    private fun getKibanaSampleDataMappingResponse(indexName: String): GetMappingsResponse {
        val mappingSourceMap = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/kibana-sample-data.json").readText()
        ).map()
        val mappingMetadata = MappingMetadata(_DOC, mappingSourceMap)

        val docMappings = ImmutableOpenMap.Builder<String, MappingMetadata>()
            .fPut(_DOC, mappingMetadata)
            .build()

        val mappings = ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>>()
            .fPut(indexName, docMappings)
            .build()

        return GetMappingsResponse(mappings)
    }
}
