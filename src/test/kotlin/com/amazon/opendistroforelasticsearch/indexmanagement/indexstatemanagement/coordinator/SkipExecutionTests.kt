/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.coordinator

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.SkipExecution
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.ClusterChangedEvent
import org.elasticsearch.cluster.ESAllocationTestCase
import org.elasticsearch.cluster.service.ClusterService
import org.junit.Before
import org.mockito.Mockito

class SkipExecutionTests : ESAllocationTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var skip: SkipExecution

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        skip = SkipExecution(client, clusterService)
    }

    fun `test cluster change event`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        Mockito.`when`(event.nodesChanged()).thenReturn(true)
        skip.clusterChanged(event)
        Mockito.verify(client).execute(Mockito.eq(NodesInfoAction.INSTANCE), Mockito.any(), Mockito.any())
    }
}