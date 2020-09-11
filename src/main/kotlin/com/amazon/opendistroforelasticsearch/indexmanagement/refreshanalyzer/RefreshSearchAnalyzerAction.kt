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

package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import org.elasticsearch.action.ActionType
import org.elasticsearch.common.io.stream.Writeable

class RefreshSearchAnalyzerAction : ActionType<RefreshSearchAnalyzerResponse>(NAME, reader) {
    companion object {
        const val NAME = "indices:admin/_refresh_search_analyzers"
        val INSTANCE = RefreshSearchAnalyzerAction()
        val reader = Writeable.Reader { inp -> RefreshSearchAnalyzerResponse(inp) }
    }

    override fun getResponseReader(): Writeable.Reader<RefreshSearchAnalyzerResponse> = reader
}
