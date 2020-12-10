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
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.delete.DeleteISMTemplateAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.get.GetISMTemplateAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.ismtemplate.put.PutISMTemplateAction
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

    fun `test index policy action name`() {
        assertNotNull(IndexPolicyAction.NAME)
        assertEquals(IndexPolicyAction.INSTANCE.name(), IndexPolicyAction.NAME)
    }

    fun `test explain action name`() {
        assertNotNull(ExplainAction.NAME)
        assertEquals(ExplainAction.INSTANCE.name(), ExplainAction.NAME)
    }

    fun `test delete policy action name`() {
        assertNotNull(DeletePolicyAction.NAME)
        assertEquals(DeletePolicyAction.INSTANCE.name(), DeletePolicyAction.NAME)
    }

    fun `test get policy action name`() {
        assertNotNull(GetPolicyAction.NAME)
        assertEquals(GetPolicyAction.INSTANCE.name(), GetPolicyAction.NAME)
    }

    fun `test get template action name`() {
        assertNotNull(GetISMTemplateAction.NAME)
        assertEquals(GetISMTemplateAction.INSTANCE.name(), GetISMTemplateAction.NAME)
    }

    fun `test put template action name`() {
        assertNotNull(PutISMTemplateAction.NAME)
        assertEquals(PutISMTemplateAction.INSTANCE.name(), PutISMTemplateAction.NAME)
    }

    fun `test delete template action name`() {
        assertNotNull(DeleteISMTemplateAction.NAME)
        assertEquals(DeleteISMTemplateAction.INSTANCE.name(), DeleteISMTemplateAction.NAME)
    }
}
