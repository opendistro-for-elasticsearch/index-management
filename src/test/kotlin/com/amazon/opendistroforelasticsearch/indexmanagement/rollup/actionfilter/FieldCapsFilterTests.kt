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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomISMFieldCapabilitiesIndexResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomISMFieldCaps
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.test.ESTestCase
import org.junit.Before

class FieldCapsFilterTests: ESTestCase() {
    private val indexNameExpressionResolver: IndexNameExpressionResolver = mock()
    private val clusterService: ClusterService = mock()
    private val clusterState: ClusterState = mock()
    private val metadata: Metadata = mock()
    private val settings: Settings = Settings.EMPTY
    private val indexMetadata: IndexMetadata = mock()
    private val rollupIndex: String = "dummy-rollupIndex"
    private val rollup: Rollup = randomRollup()

    @Before
    fun setupSettings() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(RollupSettings.ROLLUP_DASHBOARDS)))
        whenever(clusterService.state()).doReturn(clusterState)
        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollupIndex)).doReturn(indexMetadata)
    }

    fun `test rewrite unmerged response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val originalIsmResponse = ISMFieldCapabilitiesResponse(arrayOf(), mapOf(), listOf(randomISMFieldCapabilitiesIndexResponse()))
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), false) as FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertEquals("Expected merged response to be empty, indices not empty", 0, rewrittenResponse.indices.size)
        assertEquals("Expected merged response to be empty, map is empty", 0, rewrittenResponse.get().size)
        assertEquals("Expected unmerged response sizes are different", originalIsmResponse.indexResponses.size + 1, rewrittenIsmResponse.indexResponses.size)
    }

    fun `test rewrite unmerged response discarding existing response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val originalIsmResponse = ISMFieldCapabilitiesResponse(arrayOf(), mapOf(), listOf(randomISMFieldCapabilitiesIndexResponse()))
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), true) as
            FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertEquals("Expected merged response to be empty, indices not empty", 0, rewrittenResponse.indices.size)
        assertEquals("Expected merged response to be empty, map is empty", 0, rewrittenResponse.get().size)
        assertEquals("Expected unmerged response sizes are different", 1, rewrittenIsmResponse.indexResponses.size)
    }

    fun `test rewrite merged response`() {
        val fieldCapFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        val ismResponse = randomISMFieldCaps()
        val originalIsmResponse = ISMFieldCapabilitiesResponse(ismResponse.indices, ismResponse.responseMap, listOf())
        val rewrittenResponse = fieldCapFilter.rewriteResponse(originalIsmResponse.toFieldCapabilitiesResponse(), setOf(rollupIndex), true) as
            FieldCapabilitiesResponse
        val rewrittenIsmResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(rewrittenResponse)
        assertTrue("Expected unmerged response to be empty", rewrittenIsmResponse.indexResponses.isEmpty())
    }
}
