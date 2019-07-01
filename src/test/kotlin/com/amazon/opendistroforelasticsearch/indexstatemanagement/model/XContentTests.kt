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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.nonNullRandomConditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.parseDeleteActionWithType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.parseRolloverActionWithType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomDeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomRolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomTransition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.toJsonString
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase

class XContentTests : ESTestCase() {

    fun `test policy parsing`() {
        val policy = randomPolicy()

        val policyString = policy.toJsonString()
        val parsedPolicy = Policy.parseWithType(parserWithType(policyString))
        assertEquals("Round tripping Policy doesn't work", policy, parsedPolicy)
    }

    fun `test state parsing`() {
        val state = randomState()

        val stateString = state.toJsonString()
        val parsedState = State.parse(parser(stateString))
        assertEquals("Round tripping State doesn't work", state, parsedState)
    }

    fun `test transition parsing`() {
        val transition = randomTransition()

        val transitionString = transition.toJsonString()
        val parsedTransition = Transition.parse(parser(transitionString))
        assertEquals("Round tripping Transition doesn't work", transition, parsedTransition)
    }

    fun `test conditions parsing`() {
        val conditions = nonNullRandomConditions()

        val conditionsString = conditions.toJsonString()
        val parsedConditions = Conditions.parse(parser(conditionsString))
        assertEquals("Round tripping Conditions doesn't work", conditions, parsedConditions)
    }

    fun `test action config parsing`() {
        val deleteActionConfig = randomDeleteActionConfig()

        val deleteActionConfigString = deleteActionConfig.toJsonString()
        val parsedActionConfig = ActionConfig.parse((parser(deleteActionConfigString)), 0)
        assertEquals("Round tripping ActionConfig doesn't work", deleteActionConfig as ActionConfig, parsedActionConfig)
    }

    fun `test delete action config parsing`() {
        val deleteActionConfig = randomDeleteActionConfig()

        val deleteActionConfigString = deleteActionConfig.toJsonString()
        val parsedDeleteActionConfig = parseDeleteActionWithType(parserWithType(deleteActionConfigString))
        assertEquals("Round tripping DeleteActionConfig doesn't work", deleteActionConfig, parsedDeleteActionConfig)
    }

    fun `test rollover action config parsing`() {
        val rolloverActionConfig = randomRolloverActionConfig()

        val rolloverActionConfigString = rolloverActionConfig.toJsonString()
        val parsedRolloverActionConfig = parseRolloverActionWithType(parserWithType(rolloverActionConfigString))
        assertEquals("Round tripping RolloverActionConfig doesn't work", rolloverActionConfig, parsedRolloverActionConfig)
    }

    fun `test managed index config parsing`() {
        val config = randomManagedIndexConfig()
        val configTwo = config.copy(changePolicy = null)
        var configThree = config.copy()

        val configString = config.toJsonString()
        val configTwoString = configTwo.toJsonString()
        val configThreeString = configThree.toJsonString()
        val parsedConfig = ManagedIndexConfig.parseWithType(parserWithType(configString))
        val parsedConfigTwo = ManagedIndexConfig.parseWithType(parserWithType(configTwoString))
        configThree = configThree.copy(id = "some_doc_id", seqNo = 17, primaryTerm = 1)
        val parsedConfigThree =
            ManagedIndexConfig.parseWithType(parserWithType(configThreeString), configThree.id, configThree.seqNo, configThree.primaryTerm)

        assertEquals("Round tripping ManagedIndexConfig doesn't work", config, parsedConfig)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with null change policy", configTwo, parsedConfigTwo)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with id and version", configThree, parsedConfigThree)
    }

    fun `test change policy parsing`() {
        val changePolicy = randomChangePolicy()

        val changePolicyString = changePolicy.toJsonString()
        val parsedChangePolicy = ChangePolicy.parse(parser(changePolicyString))
        assertEquals("Round tripping ChangePolicy doesn't work", changePolicy, parsedChangePolicy)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
