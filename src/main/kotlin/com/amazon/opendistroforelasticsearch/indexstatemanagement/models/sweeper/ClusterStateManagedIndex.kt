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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models.sweeper

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.seqno.SequenceNumbers
import java.time.Instant

/**
 * Data class to hold index metadata from cluster state.
 *
 * This data class is used in the [com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexSweeper]
 * when reading in index metadata from cluster state and implements [ToXContentObject] for partial updates
 * of the [ManagedIndex] job document.
 */
data class ClusterStateManagedIndex(
    val index: String,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val uuid: String,
    val policyName: String
) : ToXContentObject {

    /**
     * Used in the partial update of ManagedIndices when picking up changes
     * from [com.amazon.opendistroforelasticsearch.indexstatemanagement.ManagedIndexSweeper]
     */
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .startObject(ManagedIndex.MANAGED_INDEX_TYPE)
                .field(ManagedIndex.LAST_UPDATED_TIME_FIELD, Instant.now())
                .field(ManagedIndex.CHANGE_POLICY_FIELD, ChangePolicy(policyName, null))
                .endObject()
            .endObject()
        return builder
    }
}
