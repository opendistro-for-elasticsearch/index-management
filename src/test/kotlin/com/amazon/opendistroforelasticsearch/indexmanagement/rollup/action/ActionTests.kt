/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.StopRollupAction
import org.elasticsearch.test.ESTestCase

class ActionTests : ESTestCase() {

    fun `test delete action name`() {
        assertNotNull(DeleteRollupAction.INSTANCE.name())
        assertEquals(DeleteRollupAction.INSTANCE.name(), DeleteRollupAction.NAME)
    }

    fun `test index action name`() {
        assertNotNull(IndexRollupAction.INSTANCE.name())
        assertEquals(IndexRollupAction.INSTANCE.name(), IndexRollupAction.NAME)
    }

    fun `test get action name`() {
        assertNotNull(GetRollupAction.INSTANCE.name())
        assertEquals(GetRollupAction.INSTANCE.name(), GetRollupAction.NAME)
    }

    fun `test start action name`() {
        assertNotNull(StartRollupAction.INSTANCE.name())
        assertEquals(StartRollupAction.INSTANCE.name(), StartRollupAction.NAME)
    }

    fun `test stop action name`() {
        assertNotNull(StopRollupAction.INSTANCE.name())
        assertEquals(StopRollupAction.INSTANCE.name(), StopRollupAction.NAME)
    }
}