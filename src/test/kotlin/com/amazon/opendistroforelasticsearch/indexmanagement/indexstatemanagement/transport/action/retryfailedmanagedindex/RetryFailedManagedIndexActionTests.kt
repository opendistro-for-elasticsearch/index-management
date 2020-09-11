package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.elasticsearch.test.ESTestCase
import org.junit.Assert

class RetryFailedManagedIndexActionTests : ESTestCase() {

    fun `test retry failed managed index action name`() {
        Assert.assertNotNull(RetryFailedManagedIndexAction.INSTANCE.name())
        Assert.assertEquals(RetryFailedManagedIndexAction.INSTANCE.name(), RetryFailedManagedIndexAction.NAME)
    }
}