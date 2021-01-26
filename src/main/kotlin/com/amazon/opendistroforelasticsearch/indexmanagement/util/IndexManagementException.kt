/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.util

import org.elasticsearch.ElasticsearchException
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.rest.RestStatus
import java.lang.IllegalArgumentException

class IndexManagementException(message: String, val status: RestStatus, ex: Exception) : ElasticsearchException(message, ex) {

    override fun status(): RestStatus {
        return status
    }

    companion object {
        @JvmStatic
        fun wrap(ex: Exception): ElasticsearchException {

            var friendlyMsg = ex.message as String
            var status = RestStatus.INTERNAL_SERVER_ERROR
            when (ex) {
                is IndexNotFoundException -> {
                    status = ex.status()
                    friendlyMsg = "Configuration index not found"
                }
                is IllegalArgumentException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                is ValidationException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                else -> {
                    if (!Strings.isNullOrEmpty(ex.message)) {
                        friendlyMsg = ex.message as String
                    }
                }
            }

            return IndexManagementException(friendlyMsg, status, Exception("${ex.javaClass.name}: ${ex.message}"))
        }
    }
}
