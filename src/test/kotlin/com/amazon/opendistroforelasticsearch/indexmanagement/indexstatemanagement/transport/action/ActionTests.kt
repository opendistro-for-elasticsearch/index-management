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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import org.elasticsearch.test.ESTestCase

class ActionTests : ESTestCase() {
    fun `test add policy action name`() {
        assertNotNull(AddPolicyAction.INSTANCE.name())
        assertEquals(AddPolicyAction.INSTANCE.name(), AddPolicyAction.NAME)
    }

    fun `test remove policy action name`() {
        assertNotNull(RemovePolicyAction.INSTANCE.name())
        assertEquals(RemovePolicyAction.INSTANCE.name(), RemovePolicyAction.NAME)
    }

    fun `test retry failed managed index action name`() {
        assertNotNull(RetryFailedManagedIndexAction.INSTANCE.name())
        assertEquals(RetryFailedManagedIndexAction.INSTANCE.name(), RetryFailedManagedIndexAction.NAME)
    }

    fun `test change policy action name`() {
        assertNotNull(ChangePolicyAction.NAME)
        assertEquals(ChangePolicyAction.INSTANCE.name(), ChangePolicyAction.NAME)
    }
}
