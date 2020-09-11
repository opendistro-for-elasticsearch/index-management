package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class AddPolicyActionTests : ESTestCase() {

    fun `test add policy action name`() {
        Assert.assertNotNull(AddPolicyAction.INSTANCE.name())
        Assert.assertEquals(AddPolicyAction.INSTANCE.name(), AddPolicyAction.NAME)
    }
}
