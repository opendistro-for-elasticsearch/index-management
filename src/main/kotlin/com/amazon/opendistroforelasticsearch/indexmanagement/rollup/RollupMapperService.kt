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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion._META
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.MappingMetadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.RemoteTransportException
import java.io.IOException

// TODO: Most of this has to be in a master transport action because of race conditions
// TODO: Handle existing rollup indices, validation of fields across source and target indices
//  overwriting existing rollup data, using mappings from source index
class RollupMapperService(val client: Client, val clusterService: ClusterService) {

    private val logger = LogManager.getLogger(javaClass)

    suspend fun init(rollup: Rollup): Boolean {
        if (indexExists(rollup.targetIndex)) {
            return initExistingRollupIndex(rollup)
        } else {
            createRollupTargetIndex(rollup)
        }
        return true
    }

    // If the index already exists we need to verify it's a rollup index,
    // confirm it does not conflict with existing jobs and is a valid job
    @Suppress("ReturnCount")
    private suspend fun initExistingRollupIndex(rollup: Rollup): Boolean {
        if (isRollupIndex(rollup.targetIndex)) {
            // TODO: Should this only be by ID? What happens if a user wants to delete a job and reuse?
            if (jobExistsInRollupIndex(rollup)) {
                return true
            } else {
                return updateRollupIndexMappings(rollup)
            }
        } else {
            // TODO: If we reach here this can never be resolved by the plugin -- the user
            //  has to fix the job or the index themselves so we need to permanently fail the job
            return false
        }
    }

    // This creates the target index if it doesn't already exist
    // Should reject if the target index exists and is not a rolled up index
    // TODO: Already existing rollup target index
    // TODO: error handling
    // TODO: Race condition here, need to move it into master operation
    @Suppress("ReturnCount")
    suspend fun createRollupTargetIndex(job: Rollup): Boolean {
        if (indexExists(job.targetIndex)) return isRollupIndex(job.targetIndex)
        try {
            val request = CreateIndexRequest(job.targetIndex)
                .settings(Settings.builder().put(RollupSettings.ROLLUP_INDEX.key, true).build())
                .mapping(_DOC, IndexManagementIndices.rollupTargetMappings, XContentType.JSON)
                // TODO: Perhaps we can do better than this for mappings... as it'll be dynamic for rest
                //  Can we read in the actual mappings from the source index and use that?
                //  Can it have issues with metrics? i.e. an int mapping with 3, 5, 6 added up and divided by 3 for avg is 14/3 = 4.6666
                //  What happens if the first indexing is an integer, i.e. 3 + 3 + 3 = 9/3 = 3 and it saves it as int
                //  and then the next is float and it fails or rounds it up? Does elasticsearch dynamically resolve to int?
            val response: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            // Test should not be able to put rollup metadata in non rollup index

            if (response.isAcknowledged) {
                return updateRollupIndexMappings(job)
            }
        } catch (e: RemoteTransportException) {
            logger.info("RemoteTransportException") // TODO: handle resource already exists too
        } catch (e: ResourceAlreadyExistsException) {
            logger.warn("Failed to create ${job.targetIndex} as it already exists - checking if we can add ${job.id}")
        } catch (e: Exception) {
            logger.error("Failed to create ${job.targetIndex}", e) // TODO
        }
        return false
    }

    // TODO: error handling
    // TODO: Create custom master operation to ensure we are working with the current mappings
    //  and can't have a race condition where two jobs overwrite each other
    // TODO: PutMappings needs to ensure we don't overwrite an existing rollup job meta
    //  - list current ones and ignore if it's already there
    // TODO: PUT mappings seems to overwrite all _meta data? which means we need to get and then append and then put
    @Suppress("BlockingMethodInNonBlockingContext")
    private suspend fun updateRollupIndexMappings(rollup: Rollup): Boolean {
        return withContext(Dispatchers.IO) {
            val putMappingRequest = PutMappingRequest(rollup.targetIndex).type(_DOC).source(partialRollupMappingBuilder(rollup))
            // probably can just get current mappings and parse all the existing job ids - if this job id already exists then ignore
            // should we let a person delete a job from the meta mappings of an index? they would have to make sure they deleted the data too
            val putResponse: AcknowledgedResponse = client.admin().indices().suspendUntil { putMapping(putMappingRequest, it) }
            putResponse.isAcknowledged
        }
    }

    // TODO: error handling
    // TODO: nulls, ie index response is null
    // TODO: no job exists vs has job and wrong metadata id vs has job and right metadata id
    private suspend fun jobExistsInRollupIndex(rollup: Rollup): Boolean {
        val req = GetMappingsRequest().indices(rollup.targetIndex)
        val res: GetMappingsResponse = client.admin().indices().suspendUntil { getMappings(req, it) }

        val indexMapping: MappingMetadata = res.mappings[rollup.targetIndex][_DOC]

        return ((indexMapping.sourceAsMap?.get(_META) as Map<*, *>?)?.get(ROLLUPS) as Map<*, *>?)?.containsKey(rollup.id) == true
    }

    @Throws(IOException::class)
    private fun partialRollupMappingBuilder(rollup: Rollup): XContentBuilder {
        // TODO: This is dumping *everything* of rollup into the meta and we only want a slimmed down version of it
        return XContentFactory.jsonBuilder()
            .startObject()
                .startObject(_META)
                    .startObject(ROLLUPS)
                        .field(rollup.id, rollup, XCONTENT_WITHOUT_TYPE)
                    .endObject()
                .endObject()
            .endObject()
    }

    private fun indexExists(index: String): Boolean = clusterService.state().routingTable.hasIndex(index)

    private fun isRollupIndex(index: String): Boolean = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)

    companion object {
        const val ROLLUPS = "rollups"
    }
}
