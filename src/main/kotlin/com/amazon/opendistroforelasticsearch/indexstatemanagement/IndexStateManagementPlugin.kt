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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.plugins.Plugin

internal class IndexStateManagementPlugin : JobSchedulerExtension, Plugin() {

    private val logger = LogManager.getLogger(javaClass)

    companion object {
        const val PLUGIN_NAME = "opendistro-ism"
        const val POLICY_BASE_URI = "/_opendistro/_ism/policies"
        const val INDEX_STATE_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_STATE_MANAGEMENT_TYPE = "_doc"
    }

    override fun getJobIndex(): String {
        return INDEX_STATE_MANAGEMENT_INDEX
    }

    override fun getJobType(): String {
        return INDEX_STATE_MANAGEMENT_TYPE
    }

    override fun getJobRunner(): ScheduledJobRunner {
        return ManagedIndexRunner.instance
    }

    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, version ->
            var job: ScheduledJobParameter? = null
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndex.MANAGED_INDEX_TYPE -> job = ManagedIndex.parse(xcp, id, version)
                    else -> {
                        logger.info("Unsupported document was indexed in $INDEX_STATE_MANAGEMENT_INDEX")
                    }
                }
            }
            job
        }
    }
}
