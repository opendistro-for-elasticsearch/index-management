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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.sweeper.ClusterStateManagedIndex
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import java.time.Instant
import java.time.temporal.ChronoUnit

fun createManagedIndexRequest(index: String, uuid: String, policyName: String): IndexRequest {
    val managedIndex = ManagedIndex(
        jobName = index,
        index = index,
        indexUuid = uuid,
        enabled = true,
        jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
        jobLastUpdatedTime = Instant.now(),
        jobEnabledTime = Instant.now(),
        policyName = policyName,
        policy = null,
        policyVersion = null,
        changePolicy = null
    )

    return IndexRequest(IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_INDEX,
            IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_DOC_TYPE)
            .id(uuid)
            .create(true)
            .source(managedIndex.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}

fun deleteManagedIndexRequest(uuid: String): DeleteRequest {
    return DeleteRequest(IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_INDEX,
            IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_DOC_TYPE, uuid)
}

fun updateManagedIndexRequest(clusterStateManagedIndex: ClusterStateManagedIndex): UpdateRequest {
    return UpdateRequest(IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_INDEX,
            IndexStateManagementPlugin.INDEX_STATE_MANAGEMENT_DOC_TYPE, clusterStateManagedIndex.uuid)
            .setIfPrimaryTerm(clusterStateManagedIndex.primaryTerm)
            .setIfSeqNo(clusterStateManagedIndex.seqNo)
            .doc(clusterStateManagedIndex.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
}
