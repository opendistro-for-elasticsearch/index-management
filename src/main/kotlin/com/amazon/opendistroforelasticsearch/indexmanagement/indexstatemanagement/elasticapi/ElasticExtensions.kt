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

@file:Suppress("TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.cluster.metadata.IndexMetadata

/**
 * Compares current and previous IndexMetaData to determine if we should create [ManagedIndexConfig].
 *
 * If [getPolicyID] returns null then we should not create a [ManagedIndexConfig].
 * Else if the previous IndexMetaData is null then it means this is a newly created index that should be managed.
 * Else if the previous IndexMetaData's [getPolicyID] is null then this is an existing index that had
 * a policy_id added to it.
 *
 * @param previousIndexMetaData the previous [IndexMetaData].
 * @return whether a [ManagedIndexConfig] should be created.
 */
fun IndexMetadata.shouldCreateManagedIndexConfig(previousIndexMetaData: IndexMetadata?): Boolean {
    if (this.getPolicyID() == null) return false

    return previousIndexMetaData?.getPolicyID() == null
}

/**
 * Compares current and previous IndexMetadata to determine if we should delete [ManagedIndexConfig].
 *
 * If the previous IndexMetadata is null or its [getPolicyID] returns null then there should
 * be no [ManagedIndexConfig] to delete. Else if the current [getPolicyID] returns null
 * then it means we should delete the existing [ManagedIndexConfig].
 *
 * @param previousIndexMetaData the previous [IndexMetadata].
 * @return whether a [ManagedIndexConfig] should be deleted.
 */
fun IndexMetadata.shouldDeleteManagedIndexConfig(previousIndexMetaData: IndexMetadata?): Boolean {
    if (previousIndexMetaData?.getPolicyID() == null) return false

    return this.getPolicyID() == null
}

/**
 * Checks to see if the [ManagedIndexMetaData] should be removed.
 *
 * If [getPolicyID] returns null but [ManagedIndexMetaData] is not null then the policy was removed and
 * the [ManagedIndexMetaData] remains and should be removed.
 */
fun IndexMetadata.shouldDeleteManagedIndexMetaData(): Boolean =
    this.getPolicyID() == null && this.getManagedIndexMetaData() != null

/**
 * Returns the current policy_id if it exists and is valid otherwise returns null.
 * */
fun IndexMetadata.getPolicyID(): String? {
    if (this.settings.get(ManagedIndexSettings.POLICY_ID.key).isNullOrBlank()) return null

    return this.settings.get(ManagedIndexSettings.POLICY_ID.key)
}

/**
 * Returns the current rollover_alias if it exists otherwise returns null.
 * */
fun IndexMetadata.getRolloverAlias(): String? {
    if (this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key).isNullOrBlank()) return null

    return this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key)
}

fun IndexMetadata.getClusterStateManagedIndexConfig(): ClusterStateManagedIndexConfig? {
    val index = this.index.name
    val uuid = this.index.uuid
    val policyID = this.getPolicyID() ?: return null

    return ClusterStateManagedIndexConfig(index = index, uuid = uuid, policyID = policyID)
}

fun IndexMetadata.getManagedIndexMetaData(): ManagedIndexMetaData? {
    val existingMetaDataMap = this.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)

    if (existingMetaDataMap != null) {
        return ManagedIndexMetaData.fromMap(existingMetaDataMap)
    }
    return null
}
