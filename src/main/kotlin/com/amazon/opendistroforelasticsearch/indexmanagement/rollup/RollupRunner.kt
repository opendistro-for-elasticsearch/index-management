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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool

@Suppress("TooManyFunctions")
object RollupRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var rollupMapperService: RollupMapperService
    private lateinit var rollupIndexer: RollupIndexer
    private lateinit var rollupSearchService: RollupSearchService
    private lateinit var rollupMetadataService: RollupMetadataService

    fun registerClusterService(clusterService: ClusterService): RollupRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): RollupRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): RollupRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): RollupRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): RollupRunner {
        this.settings = settings
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): RollupRunner {
        this.threadPool = threadPool
        return this
    }

    fun registerMapperService(rollupMapperService: RollupMapperService): RollupRunner {
        this.rollupMapperService = rollupMapperService
        return this
    }

    fun registerIndexer(rollupIndexer: RollupIndexer): RollupRunner {
        this.rollupIndexer = rollupIndexer
        return this
    }

    fun registerSearcher(rollupSearchService: RollupSearchService): RollupRunner {
        this.rollupSearchService = rollupSearchService
        return this
    }

    fun registerMetadataServices(
        rollupMetadataService: RollupMetadataService
    ): RollupRunner {
        this.rollupMetadataService = rollupMetadataService
        return this
    }

    fun registerConsumers(): RollupRunner {
        return this
    }

    @Suppress("ComplexMethod")
    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Rollup) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }
    }
}
