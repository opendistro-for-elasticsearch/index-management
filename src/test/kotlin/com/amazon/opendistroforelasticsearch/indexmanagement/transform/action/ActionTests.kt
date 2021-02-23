package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import org.elasticsearch.test.ESTestCase

class ActionTests : ESTestCase() {

    fun `test index transform`() {
        assertNotNull(IndexTransformAction.INSTANCE.name())
        assertEquals(IndexTransformAction.INSTANCE.name(), IndexTransformAction.NAME)
    }
}
