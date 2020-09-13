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

package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import org.elasticsearch.client.Response
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.rest.ESRestTestCase

abstract class IndexManagementRestTestCase : ESRestTestCase() {

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    @Suppress("UNCHECKED_CAST")
    protected fun getRepoPath(): String {
        val response = client()
                .makeRequest(
                        "GET",
                        "_nodes",
                        emptyMap()
                )
        assertEquals("Unable to get a nodes settings", RestStatus.OK, response.restStatus())
        return ((response.asMap()["nodes"] as HashMap<String, HashMap<String, HashMap<String, HashMap<String, Any>>>>).values.first()["settings"]!!["path"]!!["repo"] as List<String>)[0]
    }
}
