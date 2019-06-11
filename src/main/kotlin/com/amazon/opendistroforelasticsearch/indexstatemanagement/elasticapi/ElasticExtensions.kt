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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

fun XContentParser.instant(): Instant? {
    return when {
        currentToken() == XContentParser.Token.VALUE_NULL -> null
        currentToken().isValue -> Instant.ofEpochMilli(longValue())
        else -> {
            XContentParserUtils.throwUnknownToken(currentToken(), tokenLocation)
            null // unreachable
        }
    }
}

fun XContentBuilder.optionalTimeField(name: String, instant: Instant?): XContentBuilder {
    if (instant == null) {
        return nullField(name)
    }
    return this.timeField(name, name, instant.toEpochMilli())
}

/**
 * Extension function for ES 6.3 and above that duplicates the ES 6.2 XContentBuilder.string() method.
 */
fun XContentBuilder.string(): String = BytesReference.bytes(this).utf8ToString()

/**
 * Converts [ElasticsearchClient] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the ES client API.
 */
suspend fun <C : ElasticsearchClient, T> C.suspendUntil(block: C.(ActionListener<T>) -> Unit): T =
        suspendCoroutine { cont ->
            block(object : ActionListener<T> {
                override fun onResponse(response: T) = cont.resume(response)

                override fun onFailure(e: Exception) = cont.resumeWithException(e)
            })
        }

/**
 * Compares current and previous IndexMetaData to determine if we should create [ManagedIndexConfig].
 *
 * If [getPolicyName] returns null then we should not create a [ManagedIndexConfig].
 * Else if the previous IndexMetaData is null then it means this is a newly created index that should be managed.
 * Else if the previous IndexMetaData's [getPolicyName] is null then this is an existing index that had
 * a policy_name added to it.
 *
 * @param previousIndexMetaData the previous [IndexMetaData].
 * @return whether a [ManagedIndexConfig] should be created.
 */
fun IndexMetaData.shouldCreateManagedIndexConfig(previousIndexMetaData: IndexMetaData?): Boolean {
    if (this.getPolicyName() == null) return false

    return previousIndexMetaData?.getPolicyName() == null
}

/**
 * Compares current and previous IndexMetaData to determine if we should delete [ManagedIndexConfig].
 *
 * If the previous IndexMetaData is null or its [getPolicyName] returns null then there should
 * be no [ManagedIndexConfig] to delete. Else if the current [getPolicyName] returns null
 * then it means we should delete the existing [ManagedIndexConfig].
 *
 * @param previousIndexMetaData the previous [IndexMetaData].
 * @return whether a [ManagedIndexConfig] should be deleted.
 */
fun IndexMetaData.shouldDeleteManagedIndexConfig(previousIndexMetaData: IndexMetaData?): Boolean {
    if (previousIndexMetaData?.getPolicyName() == null) return false

    return this.getPolicyName() == null
}

/**
 * Compares current and previous IndexMetaData to determine if we should update [ManagedIndexConfig].
 *
 * If [getPolicyName] returns null, the previous IndexMetaData does not exist, or the previous IndexMetaData's
 * [getPolicyName] returns null then we should not update the [ManagedIndexConfig].
 * Else compare the policy_names and if they are different then we should update.
 *
 * @param previousIndexMetaData the previous [IndexMetaData].
 * @return whether a [ManagedIndexConfig] should be updated.
 */
fun IndexMetaData.shouldUpdateManagedIndexConfig(previousIndexMetaData: IndexMetaData?): Boolean {
    if (this.getPolicyName() == null || previousIndexMetaData?.getPolicyName() == null) return false

    return this.getPolicyName() != previousIndexMetaData.getPolicyName()
}

/**
 * Returns the current policy_name if it exists and is valid otherwise returns null.
 * */
fun IndexMetaData.getPolicyName(): String? {
    if (this.settings.get(ManagedIndexSettings.POLICY_NAME.key).isNullOrBlank()) return null

    return this.settings.get(ManagedIndexSettings.POLICY_NAME.key)
}

fun IndexMetaData.getClusterStateManagedIndexConfig(): ClusterStateManagedIndexConfig? {
    val index = this.index.name
    val uuid = this.index.uuid
    val policyName = this.getPolicyName()

    if (policyName == null) return null

    return ClusterStateManagedIndexConfig(index = index, uuid = uuid, policyName = policyName)
}
