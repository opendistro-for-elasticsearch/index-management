package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RemovePolicyActionTests : ESTestCase() {

    fun `test remove policy action name`() {
        Assert.assertNotNull(RemovePolicyAction.INSTANCE.name())
        Assert.assertEquals(RemovePolicyAction.INSTANCE.name(), RemovePolicyAction.NAME)
    }
}
