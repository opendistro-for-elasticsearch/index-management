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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put.PutISMTemplateResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.ismTemplates
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.putISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.removeISMTemplate
import org.apache.logging.log4j.LogManager
import org.apache.lucene.util.automaton.Operations
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.ClusterStateUpdateTask
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Priority
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.regex.Regex
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.indices.InvalidIndexTemplateException
import org.elasticsearch.rest.RestStatus
import java.util.stream.Collectors
import java.util.Locale

private val log = LogManager.getLogger(ISMTemplateService::class.java)

class ISMTemplateService @Inject constructor(
    val clusterService: ClusterService
) {
    /**
     * save ISM template to cluster state metadata
     */
    fun putISMTemplate(
        templateName: String,
        template: ISMTemplate,
        masterTimeout: TimeValue,
        listener: ActionListener<PutISMTemplateResponse>
    ) {
        clusterService.submitStateUpdateTask(
            IndexManagementPlugin.PLUGIN_NAME,
            object : ClusterStateUpdateTask(Priority.NORMAL) {
                override fun execute(currentState: ClusterState): ClusterState {
                    return addISMTemplate(currentState, templateName, template)
                }

                override fun onFailure(source: String, e: Exception) {
                    listener.onFailure(e)
                }

                override fun timeout(): TimeValue = masterTimeout

                override fun clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
                    var status = RestStatus.CREATED
                    val oldTemplate = oldState.metadata.ismTemplates()[templateName]
                    if (oldTemplate != null) {
                        status = RestStatus.OK
                    }
                    listener.onResponse(PutISMTemplateResponse(templateName, template, status))
                }
            }
        )
    }

    fun addISMTemplate(currentState: ClusterState, templateName: String, template: ISMTemplate): ClusterState {
        val existingTemplates = currentState.metadata.ismTemplates()
        val existingTemplate = existingTemplates[templateName]

        if (template == existingTemplate) return currentState

        // find templates with overlapping index pattern
        val overlaps = findConflictingISMTemplates(templateName, template.indexPatterns, template.priority, existingTemplates)
        if (overlaps.isNotEmpty()) {
            val esg = "new ism template $templateName has index pattern ${template.indexPatterns} " +
                "matching existing templates ${overlaps.entries.stream().map { "${it.key} => ${it.value}" }.collect(Collectors.joining(","))}," +
                " please use a different priority than ${template.priority}"
            throw IllegalArgumentException(esg)
        }

        validateFormat(templateName, template.indexPatterns)

        return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata())
                .putISMTemplate(templateName, template, existingTemplates)).build()
    }

    /**
     * remove ISM template from cluster state metadata
     */
    fun deleteISMTemplate(templateName: String, masterTimeout: TimeValue, listener: ActionListener<AcknowledgedResponse>) {
        clusterService.submitStateUpdateTask(
            IndexManagementPlugin.PLUGIN_NAME,
            object : ClusterStateUpdateTask(Priority.NORMAL) {
                override fun execute(currentState: ClusterState): ClusterState {
                    val existingTemplates = currentState.metadata.ismTemplates()
                    return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata)
                            .removeISMTemplate(templateName, existingTemplates)).build()
                }

                override fun onFailure(source: String, e: Exception) {
                    listener.onFailure(e)
                }

                override fun timeout(): TimeValue = masterTimeout

                override fun clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) {
                    listener.onResponse(AcknowledgedResponse(true))
                }
            }
        )
    }

    companion object {
        /**
         * find the matching template for the index
         *
         * filter out hidden index
         * filter out older index than template lastUpdateTime
         *
         * @param ismTemplates current ISM templates saved in metadata
         * @param indexMetadata cluster state index metadata
         * @return template name matching with given index
         */
        @Suppress("ReturnCount")
        fun findMatchingISMTemplate(ismTemplates: Map<String, ISMTemplate>, indexMetadata: IndexMetadata): String? {
            val indexName = indexMetadata.index.name

            // don't include hidden index
            val isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings)
            log.info("index $indexName is hidden $isHidden")
            if (isHidden) return null

            // only process indices created after template
            // traverse all ism templates for matching ones
            val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, indexName) }
            val matchedTemplates = mutableMapOf<ISMTemplate, String>()
            ismTemplates.filter { (_, template) ->
                log.info("index create after template? ${template.lastUpdatedTime.toEpochMilli() < indexMetadata.creationDate}")
                template.lastUpdatedTime.toEpochMilli() < indexMetadata.creationDate
            }.forEach { (templateName, template) ->
                val matched = template.indexPatterns.stream().anyMatch(patternMatchPredicate)
                if (matched) matchedTemplates[template] = templateName
            }

            if (matchedTemplates.isEmpty()) return null

            // sort by template priority
            val winner = matchedTemplates.keys.maxBy { it.priority }
            log.info("winner with highest priority is $winner")
            return matchedTemplates[winner]
        }

        /**
         * validate the template Name and indexPattern provided in the template
         * reusing ES validate function in MetadataIndexTemplateService
         */
        @Suppress("ComplexMethod")
        fun validateFormat(templateName: String, indexPatterns: List<String>) {
            val validationErrors = mutableListOf<String>()
            if (templateName.contains(" ")) {
                validationErrors.add("name must not contain a space")
            }
            if (templateName.contains(",")) {
                validationErrors.add("name must not contain a ','")
            }
            if (templateName.contains("#")) {
                validationErrors.add("name must not contain a '#'")
            }
            if (templateName.contains("*")) {
                validationErrors.add("name must not contain a '*'")
            }
            if (templateName.startsWith("_")) {
                validationErrors.add("name must not start with '_'")
            }
            if (templateName.toLowerCase(Locale.ROOT) != templateName) {
                validationErrors.add("name must be lower cased")
            }
            for (indexPattern in indexPatterns) {
                if (indexPattern.contains(" ")) {
                    validationErrors.add("index_patterns [$indexPattern] must not contain a space")
                }
                if (indexPattern.contains(",")) {
                    validationErrors.add("index_pattern [$indexPattern] must not contain a ','")
                }
                if (indexPattern.contains("#")) {
                    validationErrors.add("index_pattern [$indexPattern] must not contain a '#'")
                }
                if (indexPattern.contains(":")) {
                    validationErrors.add("index_pattern [$indexPattern] must not contain a ':'")
                }
                if (indexPattern.startsWith("_")) {
                    validationErrors.add("index_pattern [$indexPattern] must not start with '_'")
                }
                if (!Strings.validFileNameExcludingAstrix(indexPattern)) {
                    validationErrors.add("index_pattern [" + indexPattern + "] must not contain the following characters " +
                            Strings.INVALID_FILENAME_CHARS)
                }
            }

            if (validationErrors.size > 0) {
                val validationException = ValidationException()
                validationException.addValidationErrors(validationErrors)
                throw InvalidIndexTemplateException(templateName, validationException.message)
            }
        }

        /**
         * find templates whose index patterns overlap with given template
         *
         * @return map of overlapping template name to its index patterns
         */
        @Suppress("SpreadOperator")
        fun findConflictingISMTemplates(
            candidate: String,
            indexPatterns: List<String>,
            priority: Int,
            ismTemplates: Map<String, ISMTemplate>
        ): Map<String, List<String>> {
            val automaton1 = Regex.simpleMatchToAutomaton(*indexPatterns.toTypedArray())
            val overlappingTemplates = mutableMapOf<String, List<String>>()

            // focus on template with same priority
            ismTemplates.filter { it.value.priority == priority }.forEach { (templateName, template) ->
                val automaton2 = Regex.simpleMatchToAutomaton(*template.indexPatterns.toTypedArray())
                if (!Operations.isEmpty(Operations.intersection(automaton1, automaton2))) {
                    log.info("existing template $templateName overlaps candidate $candidate")
                    overlappingTemplates[templateName] = template.indexPatterns
                }
            }
            overlappingTemplates.remove(candidate)

            return overlappingTemplates
        }
    }
}
