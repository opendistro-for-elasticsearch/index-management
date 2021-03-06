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

@file:Suppress("MagicNumber", "ComplexMethod", "NestedBlockDepth", "TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateISMTemplateRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexManagementException
import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.apache.lucene.util.automaton.Operations
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateResponse
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.Template
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.regex.Regex
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import java.time.Instant

private val log = LogManager.getLogger("ISMTemplateService")

/**
 * find the matching policy for the given index
 *
 * return early if it's hidden index
 * filter out templates that were last updated after the index creation time
 *
 * @return policyID
 */
@Suppress("ReturnCount")
fun Map<String, List<ISMTemplate>>.findMatchingPolicy(indexName: String, indexCreationDate: Long, isHiddenIndex: Boolean): String? {
    if (this.isEmpty()) return null
    // don't include hidden index
    if (isHiddenIndex) return null

    // only process indices created after template
    // traverse all ism templates for matching ones
    val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, indexName) }
    var matchedPolicy: String? = null
    var highestPriority: Int = -1

    this.forEach { (policyID, templateList) ->
        templateList.filter { it.lastUpdatedTime.toEpochMilli() < indexCreationDate }
            .forEach {
                if (it.indexPatterns.stream().anyMatch(patternMatchPredicate)) {
                    if (highestPriority < it.priority) {
                        highestPriority = it.priority
                        matchedPolicy = policyID
                    } else if (highestPriority == it.priority) {
                        log.warn("Warning: index $indexName matches [$matchedPolicy, $policyID]")
                    }
                }
            }
    }

    return matchedPolicy
}

/**
 * validate the template Name and indexPattern provided in the template
 *
 * get the idea from ES validate function in MetadataIndexTemplateService
 * acknowledge https://github.com/a2lin who should be the first contributor
 */
@Suppress("ComplexMethod")
fun validateFormat(indexPatterns: List<String>): ElasticsearchException? {
    val indexPatternFormatErrors = mutableListOf<String>()
    for (indexPattern in indexPatterns) {
        if (indexPattern.contains("#")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a '#'")
        }
        if (indexPattern.contains(":")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a ':'")
        }
        if (indexPattern.startsWith("_")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not start with '_'")
        }
        if (!Strings.validFileNameExcludingAstrix(indexPattern)) {
            indexPatternFormatErrors.add("index_pattern [" + indexPattern + "] must not contain the following characters " +
                Strings.INVALID_FILENAME_CHARS)
        }
    }

    if (indexPatternFormatErrors.size > 0) {
        val validationException = ValidationException()
        validationException.addValidationErrors(indexPatternFormatErrors)
        return IndexManagementException.wrap(validationException)
    }
    return null
}

fun List<ISMTemplate>.findSelfConflictingTemplates(): Pair<List<String>, List<String>>? {
    val priorityToTemplates = mutableMapOf<Int, List<ISMTemplate>>()
    this.forEach {
        val templateList = priorityToTemplates[it.priority]
        if (templateList != null) {
            priorityToTemplates[it.priority] = templateList.plus(it)
        } else {
            priorityToTemplates[it.priority] = mutableListOf(it)
        }
    }
    priorityToTemplates.forEach { (_, templateList) ->
        // same priority
        val indexPatternsList = templateList.map { it.indexPatterns }
        if (indexPatternsList.size > 1) {
            indexPatternsList.forEachIndexed { ind, indexPatterns ->
                val comparePatterns = indexPatternsList.subList(ind + 1, indexPatternsList.size).flatten()
                if (overlapping(indexPatterns, comparePatterns)) {
                    return indexPatterns to comparePatterns
                }
            }
        }
    }

    return null
}

@Suppress("SpreadOperator")
fun overlapping(p1: List<String>, p2: List<String>): Boolean {
    if (p1.isEmpty() || p2.isEmpty()) return false
    val a1 = Regex.simpleMatchToAutomaton(*p1.toTypedArray())
    val a2 = Regex.simpleMatchToAutomaton(*p2.toTypedArray())
    return !Operations.isEmpty(Operations.intersection(a1, a2))
}

/**
 * find policy templates whose index patterns overlap with given template
 *
 * @return map of overlapping template name to its index patterns
 */
fun Map<String, List<ISMTemplate>>.findConflictingPolicyTemplates(
    candidate: String,
    indexPatterns: List<String>,
    priority: Int
): Map<String, List<String>> {
    val overlappingTemplates = mutableMapOf<String, List<String>>()

    this.forEach { (policyID, templateList) ->
        templateList.filter { it.priority == priority }
            .map { it.indexPatterns }
            .forEach {
                if (overlapping(indexPatterns, it)) {
                    overlappingTemplates[policyID] = it
                }
            }
    }
    overlappingTemplates.remove(candidate)
    return overlappingTemplates
}

@OpenForTesting
class ISMTemplateService(
    private val client: Client,
    private val clusterService: ClusterService,
    private val xContentRegistry: NamedXContentRegistry,
    private val imIndices: IndexManagementIndices
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile final var finishFlag = false
        private set
    fun reenableTemplateMigration() { finishFlag = false }

    @Volatile var runTimeCounter = 0

    private var ismTemplateMap = mutableMapOf<policyID, MutableList<ISMTemplate>>()
    private val v1TemplatesWithPolicyID = mutableMapOf<templateName, V1TemplateCache>()

    private val negOrderToPositive = mutableMapOf<Int, Int>()
    private val v1orderToTemplatesName = mutableMapOf<Int, MutableList<templateName>>()
    private val v1orderToBucketIncrement = mutableMapOf<Int, Int>()

    private val policiesToUpdate = mutableMapOf<policyID, seqNoPrimaryTerm>()
    private val policiesFailedToUpdate = mutableMapOf<policyID, BulkItemResponse.Failure>()
    private lateinit var lastUpdatedTime: Instant

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    suspend fun doMigration(timeStamp: Instant) {
        if (runTimeCounter >= 10) {
            stopMigration(-2)
            return
        }
        logger.info("Doing ISM template migration ${++runTimeCounter} time.")
        cleanCache()

        lastUpdatedTime = timeStamp.minusSeconds(3600)
        logger.info("Use $lastUpdatedTime as migrating ISM template last_updated_time")

        getIndexTemplates()
        logger.info("ISM templates: $ismTemplateMap")

        getISMPolicies()
        logger.info("Policies to update: ${policiesToUpdate.keys}")

        updateISMPolicies()

        if (policiesToUpdate.isEmpty()) {
            stopMigration(-1)
        }
    }

    private fun stopMigration(successFlag: Long) {
        finishFlag = true
        val newSetting = Settings.builder().put(ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL.key, successFlag)
        val request = ClusterUpdateSettingsRequest().persistentSettings(newSetting)
        client.admin().cluster().updateSettings(request, updateSettingListener())
        logger.info("Failure experienced when migrating ISM Template and update ISM policies: $policiesFailedToUpdate")
        // TODO what if update setting failed, cannot reset to -1/-2
        runTimeCounter = 0
    }

    private fun updateSettingListener(): ActionListener<ClusterUpdateSettingsResponse> {
        return object : ActionListener<ClusterUpdateSettingsResponse> {
            override fun onFailure(e: Exception) {
                logger.error("Failed to update template migration setting", e)
            }

            override fun onResponse(response: ClusterUpdateSettingsResponse) {
                if (!response.isAcknowledged) {
                    logger.error("Update template migration setting is not acknowledged")
                } else {
                    logger.info("Successfully update template migration setting")
                }
            }
        }
    }

    private suspend fun getIndexTemplates() {
        processNegativeOrder()

        bucketizeV1TemplatesByOrder()
        populateBucketPriority()
        populateV1Template()

        clusterService.state().metadata.templatesV2().forEach {
            val template = it.value
            val indexPatterns = template.indexPatterns()
            val priority = template.priorityOrZero().toInt()
            val policyIDSetting = simulateTemplate(it.key)
            if (policyIDSetting != null) {
                populateV2ISMTemplateMap(policyIDSetting, indexPatterns, priority)
            }
        }
    }

    // old v1 template can have negative priority
    // map the negative priority to non-negative value
    private fun processNegativeOrder() {
        val negOrderSet = mutableSetOf<Int>()
        clusterService.state().metadata.templates.forEach {
            val policyIDSetting = ManagedIndexSettings.POLICY_ID.get(it.value.settings())
            if (policyIDSetting != null) {
                val priority = it.value.order
                if (priority < 0) {
                    negOrderSet.add(priority)
                }
                // cache pattern and policyID for v1 template
                v1TemplatesWithPolicyID[it.key] = V1TemplateCache(it.value.patterns(), 0, policyIDSetting)
            }
        }
        val sorted = negOrderSet.sorted()
        var p = 0
        for (i in sorted) {
            negOrderToPositive[i] = p++
        }
    }

    private fun normalizePriority(order: Int): Int {
        if (order < 0) return negOrderToPositive[order] ?: 0
        return order + (negOrderToPositive.size)
    }

    private fun bucketizeV1TemplatesByOrder() {
        clusterService.state().metadata.templates.forEach {
            val v1TemplateCache = v1TemplatesWithPolicyID[it.key]
            if (v1TemplateCache != null) {
                val priority = normalizePriority(it.value.order)
                // cache the non-negative priority
                v1TemplatesWithPolicyID[it.key] = v1TemplateCache.copy(order = priority)

                val bucket = v1orderToTemplatesName[priority]
                if (bucket == null) {
                    v1orderToTemplatesName[priority] = mutableListOf(it.key)
                } else {
                    // add later one to start of the list
                    bucket.add(0, it.key)
                }
            }
        }
    }

    private fun populateBucketPriority() {
        v1orderToTemplatesName.forEach { (order, templateNames) ->
            var increase = 0
            templateNames.forEach {
                val v1TemplateCache = v1TemplatesWithPolicyID[it]
                if (v1TemplateCache != null) {
                    val cachePriority = v1TemplateCache.order
                    v1TemplatesWithPolicyID[it] = v1TemplateCache
                        .copy(order = cachePriority + increase)
                }
                increase++
            }
            v1orderToBucketIncrement[order] = templateNames.size - 1
        }
    }

    private fun populateV1Template() {
        val allOrders = v1orderToTemplatesName.keys.toList().sorted()
        allOrders.forEachIndexed { ind, order ->
            val smallerOrders = allOrders.subList(0, ind)
            val increments = smallerOrders.mapNotNull { v1orderToBucketIncrement[it] }.sum()

            val templates = v1orderToTemplatesName[order]
            templates?.forEach {
                val v1TemplateCache = v1TemplatesWithPolicyID[it]
                if (v1TemplateCache != null) {
                    val policyID = v1TemplateCache.policyID
                    val indexPatterns = v1TemplateCache.patterns
                    val priority = v1TemplateCache.order + increments
                    saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, priority, lastUpdatedTime))
                }
            }
        }
    }

    private fun saveISMTemplateToMap(policyID: String, ismTemplate: ISMTemplate) {
        val policyToISMTemplate = ismTemplateMap[policyID]
        if (policyToISMTemplate != null) {
            policyToISMTemplate.add(ismTemplate)
        } else {
            ismTemplateMap[policyID] = mutableListOf(ismTemplate)
        }
    }

    private suspend fun simulateTemplate(templateName: String): String? {
        val request = SimulateTemplateAction.Request(templateName)
        val response: SimulateIndexTemplateResponse =
            client.suspendUntil { execute(SimulateTemplateAction.INSTANCE, request, it) }

        var policyIDSetting: String? = null
        withContext(Dispatchers.IO) {
            val out = BytesStreamOutput().also { response.writeTo(it) }
            val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
            val resolvedTemplate = sin.readOptionalWriteable(::Template)
            if (resolvedTemplate != null) {
                policyIDSetting = ManagedIndexSettings.POLICY_ID.get(resolvedTemplate.settings())
            }
        }
        return policyIDSetting
    }

    private fun populateV2ISMTemplateMap(policyID: String, indexPatterns: List<String>, priority: Int) {
        var v1Increment = 0
        val v1MinOrder = v1orderToBucketIncrement.keys.max()
        if (v1MinOrder != null) {
            v1Increment = v1MinOrder + v1orderToBucketIncrement.values.sum()
        }

        saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, normalizePriority(priority) + v1Increment, lastUpdatedTime))
    }

    private suspend fun getISMPolicies() {
        if (ismTemplateMap.isEmpty()) return

        val mReq = MultiGetRequest()
        ismTemplateMap.keys.forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }
        try {
            val mRes: MultiGetResponse = client.suspendUntil { multiGet(mReq, it) }
            policiesToUpdate.clear()
            mRes.forEach {
                if (it.response != null && !it.response.isSourceEmpty && !it.isFailed) {
                    val response = it.response
                    var policy: Policy? = null
                    try {
                        policy = XContentHelper.createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef,
                            XContentType.JSON
                        ).use { xcp ->
                            xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, Policy.Companion::parse)
                        }
                    } catch (e: Exception) {
                        logger.error("Failed to parse policy [${response.id}] when migrating templates", e)
                    }

                    if (policy?.ismTemplate == null) {
                        policiesToUpdate[it.id] = Pair(response.seqNo, response.primaryTerm)
                    }
                }
            }
        } catch (e: ActionRequestValidationException) {
            logger.warn("ISM config index not exists when migrating templates.")
        }
    }

    private suspend fun updateISMPolicies() {
        if (policiesToUpdate.isEmpty()) return

        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return
        }

        val requests = mutableListOf<UpdateRequest>()
        policiesToUpdate.forEach { policyID, (seqNo, priTerm) ->
            val ismTemplates = ismTemplateMap[policyID]
            if (ismTemplates != null)
                requests.add(updateISMTemplateRequest(policyID, ismTemplates, seqNo, priTerm))
        }
        var requestsToRetry: List<DocWriteRequest<*>> = requests

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkReq = BulkRequest().add(requestsToRetry)
            val bulkRes: BulkResponse = client.suspendUntil { bulk(bulkReq, it) }
            val failedResponses = (bulkRes.items ?: arrayOf()).filter { it.isFailed }

            val retryItems = mutableListOf<Int>()
            bulkRes.items.forEach {
                if (it.isFailed) {
                    if (it.status() == RestStatus.TOO_MANY_REQUESTS) {
                        retryItems.add(it.itemId)
                    } else {
                        logger.error("Failed to update template for policy [${it.id}], ${it.failureMessage}")
                        policiesFailedToUpdate[it.id] = it.failure
                    }
                } else {
                    policiesToUpdate.remove(it.id)
                    policiesFailedToUpdate.remove(it.id)
                }
            }
            requestsToRetry = retryItems.map { bulkReq.requests()[it] as UpdateRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToElastic(retryCause)
            }
        }
    }

    private fun cleanCache() {
        ismTemplateMap.clear()
        v1TemplatesWithPolicyID.clear()
        v1orderToTemplatesName.clear()
        v1orderToBucketIncrement.clear()
        negOrderToPositive.clear()
        policiesToUpdate.clear()
    }
}

data class V1TemplateCache(
    val patterns: List<String>,
    val order: Int,
    val policyID: String
)

typealias policyID = String
typealias templateName = String
typealias seqNoPrimaryTerm = Pair<Long, Long>
