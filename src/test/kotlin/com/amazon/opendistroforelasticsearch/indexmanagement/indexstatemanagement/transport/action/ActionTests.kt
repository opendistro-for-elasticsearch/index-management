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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class ActionTests : ESTestCase() {
    fun `test add policy action name`() {
        Assert.assertNotNull(AddPolicyAction.INSTANCE.name())
        Assert.assertEquals(AddPolicyAction.INSTANCE.name(), AddPolicyAction.NAME)
    }

    fun `test remove policy action name`() {
        Assert.assertNotNull(RemovePolicyAction.INSTANCE.name())
        Assert.assertEquals(RemovePolicyAction.INSTANCE.name(), RemovePolicyAction.NAME)
    }
}