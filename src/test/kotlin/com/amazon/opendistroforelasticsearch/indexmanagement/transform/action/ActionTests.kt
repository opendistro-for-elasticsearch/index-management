/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get.GetTransformsAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index.IndexTransformAction
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.preview.PreviewTransformAction
import org.elasticsearch.test.ESTestCase

class ActionTests : ESTestCase() {

    fun `test delete transform name`() {
        assertNotNull(DeleteTransformsAction.INSTANCE.name())
        assertEquals(DeleteTransformsAction.INSTANCE.name(), DeleteTransformsAction.NAME)
    }

    fun `test index transform name`() {
        assertNotNull(IndexTransformAction.INSTANCE.name())
        assertEquals(IndexTransformAction.INSTANCE.name(), IndexTransformAction.NAME)
    }

    fun `test preview transform name`() {
        assertNotNull(PreviewTransformAction.INSTANCE.name())
        assertEquals(PreviewTransformAction.INSTANCE.name(), PreviewTransformAction.NAME)
    }

    fun `test get transform name`() {
        assertNotNull(GetTransformAction.INSTANCE.name())
        assertEquals(GetTransformAction.INSTANCE.name(), GetTransformAction.NAME)
    }

    fun `test get transforms name`() {
        assertNotNull(GetTransformsAction.INSTANCE.name())
        assertEquals(GetTransformsAction.INSTANCE.name(), GetTransformsAction.NAME)
    }
}
