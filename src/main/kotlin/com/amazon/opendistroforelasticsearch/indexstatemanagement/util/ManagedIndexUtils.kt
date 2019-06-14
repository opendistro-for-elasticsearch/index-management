/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
@file:JvmName("ManagedIndexUtils")

package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

fun createManagedIndexRequest(index: String, uuid: String, policyName: String): IndexRequest {
    val managedIndexConfig = ManagedIndexConfig(
        jobName = index,
        index = index,
        indexUuid = uuid,
        enabled = true,
        jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
        jobLastUpdatedTime = Instant.now(),
        jobEnabledTime = Instant.now(),
        policyName = policyName,
        policy = null,
        policySeqNo = null,
        policyPrimaryTerm = null,
        changePolicy = null
    )

    return IndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
            .id(uuid)
            .create(true)
            .source(managedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

fun deleteManagedIndexRequest(uuid: String): DeleteRequest {
    return DeleteRequest(INDEX_STATE_MANAGEMENT_INDEX, uuid)
}

fun updateManagedIndexRequest(clusterStateManagedIndexConfig: ClusterStateManagedIndexConfig): UpdateRequest {
    return UpdateRequest(INDEX_STATE_MANAGEMENT_INDEX,
            clusterStateManagedIndexConfig.uuid)
            .setIfPrimaryTerm(clusterStateManagedIndexConfig.primaryTerm)
            .setIfSeqNo(clusterStateManagedIndexConfig.seqNo)
            .doc(clusterStateManagedIndexConfig.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

/**
 * Creates IndexRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist in the cluster state that do not yet exist in [INDEX_STATE_MANAGEMENT_INDEX]
 * which means we need to create the [ManagedIndexConfig].
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getCreateManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return clusterStateManagedIndexConfigs.filter { (uuid) ->
        !currentManagedIndexConfigs.containsKey(uuid)
    }.map { createManagedIndexRequest(it.value.index, it.value.uuid, it.value.policyName) }
}

/**
 * Creates DeleteRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist in [INDEX_STATE_MANAGEMENT_INDEX] that do not exist in the cluster state
 * anymore which means we need to delete the [ManagedIndexConfig].
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getDeleteManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return currentManagedIndexConfigs.filter { (uuid) ->
        !clusterStateManagedIndexConfigs.containsKey(uuid)
    }.map { deleteManagedIndexRequest(it.value.uuid) }
}

/**
 * Creates UpdateRequests for [ManagedIndexConfig].
 *
 * Finds ManagedIndices that exist both in cluster state and in [INDEX_STATE_MANAGEMENT_INDEX] that
 * need to be updated. We know a [ManagedIndexConfig] needs to be updated when the policyName differs between
 * the [ClusterStateManagedIndexConfig] and the [SweptManagedIndexConfig]. And we know
 * a [ManagedIndexConfig] has not yet been updated if it's ChangePolicy does not match the new policyName.
 *
 * @param clusterStateManagedIndexConfigs map of IndexUuid to [ClusterStateManagedIndexConfig].
 * @param currentManagedIndexConfigs map of IndexUuid to [SweptManagedIndexConfig].
 * @return list of [DocWriteRequest].
 */
@OpenForTesting
fun getUpdateManagedIndexRequests(
    clusterStateManagedIndexConfigs: Map<String, ClusterStateManagedIndexConfig>,
    currentManagedIndexConfigs: Map<String, SweptManagedIndexConfig>
): List<DocWriteRequest<*>> {
    return clusterStateManagedIndexConfigs.asSequence()
            .filter { (uuid, clusterConfig) ->
                val sweptConfig = currentManagedIndexConfigs[uuid]
                sweptConfig != null &&
                        // Verify they have different policy names which means we should update it
                        sweptConfig.policyName != clusterConfig.policyName &&
                        // Verify it is not already being updated
                        sweptConfig.changePolicy?.policyName != clusterConfig.policyName
            }
            .map {
                val sweptConfig = currentManagedIndexConfigs[it.key]
                if (sweptConfig == null) {
                    updateManagedIndexRequest(it.value)
                } else {
                    updateManagedIndexRequest(
                            it.value.copy(seqNo = sweptConfig.seqNo,
                                    primaryTerm = sweptConfig.primaryTerm)
                    )
                }
            }.toList()
}

fun getSweptManagedIndexSearchRequest(): SearchRequest {
    val boolQueryBuilder = BoolQueryBuilder().filter(QueryBuilders.existsQuery(ManagedIndexConfig.MANAGED_INDEX_TYPE))
    return SearchRequest()
            .indices(INDEX_STATE_MANAGEMENT_INDEX)
            .source(SearchSourceBuilder.searchSource()
                    // TODO: Get all ManagedIndices at once or split into searchAfter queries?
                    .size(ManagedIndexCoordinator.MAX_HITS)
                    .seqNoAndPrimaryTerm(true)
                    .fetchSource(
                            arrayOf(
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.INDEX_UUID_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.POLICY_NAME_FIELD}",
                                    "${ManagedIndexConfig.MANAGED_INDEX_TYPE}.${ManagedIndexConfig.CHANGE_POLICY_FIELD}"
                            ),
                            emptyArray()
                    )
                    .query(boolQueryBuilder))
}
