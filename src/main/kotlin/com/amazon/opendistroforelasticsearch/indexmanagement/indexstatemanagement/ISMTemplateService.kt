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

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.filterNotNullValues
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexManagementException
import org.apache.logging.log4j.LogManager
import org.apache.lucene.util.automaton.Operations
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.regex.Regex

private val log = LogManager.getLogger(ISMTemplateService::class.java)

class ISMTemplateService {
    companion object {
        /**
         * find the matching policy based on ISM template field for the given index
         *
         * filter out hidden index
         * filter out older index than template lastUpdateTime
         *
         * @param ismTemplates current ISM templates saved in metadata
         * @param indexMetadata cluster state index metadata
         * @return policyID
         */
        @Suppress("ReturnCount")
        fun findMatchingPolicy(ismTemplates: Map<String, ISMTemplate>, indexMetadata: IndexMetadata): String? {
            if (ismTemplates.isEmpty()) return null

            val indexName = indexMetadata.index.name

            // don't include hidden index
            val isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings)
            if (isHidden) return null

            // only process indices created after template
            // traverse all ism templates for matching ones
            val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, indexName) }
            var matchedPolicy: String? = null
            var highestPriority: Int = -1
            ismTemplates.filter { (_, template) ->
                template.lastUpdatedTime.toEpochMilli() < indexMetadata.creationDate
            }.forEach { (policyID, template) ->
                val matched = template.indexPatterns.stream().anyMatch(patternMatchPredicate)
                if (matched && highestPriority < template.priority) {
                    highestPriority = template.priority
                    matchedPolicy = policyID
                }
            }

            return matchedPolicy
        }

        /**
         * validate the template Name and indexPattern provided in the template
         * reusing ES validate function in MetadataIndexTemplateService
         */
        @Suppress("ComplexMethod")
        fun validateFormat(indexPatterns: List<String>): ElasticsearchException? {
            val validationErrors = mutableListOf<String>()
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
                return IndexManagementException.wrap(validationException)
            }
            return null
        }

        /**
         * find policy templates whose index patterns overlap with given template
         *
         * @return map of overlapping template name to its index patterns
         */
        @Suppress("SpreadOperator")
        fun findConflictingPolicyTemplates(
            candidate: String,
            indexPatterns: List<String>,
            priority: Int,
            ismTemplates: Map<String, ISMTemplate?>
        ): Map<String, List<String>> {
            val automaton1 = Regex.simpleMatchToAutomaton(*indexPatterns.toTypedArray())
            val overlappingTemplates = mutableMapOf<String, List<String>>()

            // focus on template with same priority
            ismTemplates.filterNotNullValues()
                .filter { it.value.priority == priority }.forEach { (policyID, template) ->
                val automaton2 = Regex.simpleMatchToAutomaton(*template.indexPatterns.toTypedArray())
                if (!Operations.isEmpty(Operations.intersection(automaton1, automaton2))) {
                    log.info("existing ism_template in $policyID overlaps candidate $candidate")
                    overlappingTemplates[policyID] = template.indexPatterns
                }
            }
            overlappingTemplates.remove(candidate)

            return overlappingTemplates
        }
    }
}
