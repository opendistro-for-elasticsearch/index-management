package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class ChangePolicyActionTests : ESTestCase() {

    fun `test change policy action name`() {
        Assert.assertNotNull(ChangePolicyAction.NAME)
        Assert.assertEquals(ChangePolicyAction.INSTANCE.name(), ChangePolicyAction.NAME)
    }
}