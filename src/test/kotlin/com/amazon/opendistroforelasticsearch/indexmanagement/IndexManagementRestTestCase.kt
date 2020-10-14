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

import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.rest.ESRestTestCase
import org.junit.AfterClass
import org.junit.rules.DisableOnDebug
import java.nio.file.Files
import java.nio.file.Path
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

abstract class IndexManagementRestTestCase : ESRestTestCase() {

    protected val isDebuggingTest = DisableOnDebug(null).isDebugging
    protected val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()
    protected val isMultiNode = System.getProperty("cluster.number_of_nodes", "1").toInt() > 1

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun getRepoPath(): String = System.getProperty("tests.path.repo")

    protected fun assertIndexExists(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does not exist.", RestStatus.OK, response.restStatus())
    }

    protected fun assertIndexDoesNotExist(index: String) {
        val response = client().makeRequest("HEAD", index)
        assertEquals("Index $index does exist.", RestStatus.NOT_FOUND, response.restStatus())
    }

    protected fun verifyIndexSchemaVersion(index: String, expectedVersion: Int) {
        val indexMapping = client().getIndexMapping(index)
        val indexName = indexMapping.keys.toList()[0]
        val mappings = indexMapping.stringMap(indexName)?.stringMap("mappings")
        var version = 0
        if (mappings!!.containsKey("_meta")) {
            val meta = mappings.stringMap("_meta")
            if (meta!!.containsKey("schema_version")) version = meta.get("schema_version") as Int
        }
        assertEquals(expectedVersion, version)
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.stringMap(key: String): Map<String, Any>? {
        val map = this as Map<String, Map<String, Any>>
        return map[key]
    }

    fun RestClient.getIndexMapping(index: String): Map<String, Any> {
        val response = this.makeRequest("GET", "$index/_mapping")
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    /**
     * Inserts [docCount] sample documents into [index], optionally waiting [delay] milliseconds
     * in between each insertion
     */
    protected fun insertSampleData(index: String, docCount: Int, delay: Long = 0, jsonString: String = "{ \"test_field\": \"test_value\" }") {
        for (i in 1..docCount) {
            val request = Request("POST", "/$index/_doc/?refresh=true")
            request.setJsonEntity(jsonString)
            client().performRequest(request)

            Thread.sleep(delay)
        }
    }

    protected fun insertSampleBulkData(index: String, bulkJsonString: String) {
        val request = Request("POST", "/$index/_bulk/?refresh=true")
        request.setJsonEntity(bulkJsonString)
        request.options = RequestOptions.DEFAULT.toBuilder().addHeader("content-type", "application/x-ndjson").build()
        val res = client().performRequest(request)
        assertEquals(RestStatus.OK, res.restStatus())
    }

    /**
     * Indexes 5k documents of the open NYC taxi dataset
     *
     * Example headers and document values
     * VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
     * 1,2019-01-01 00:46:40,2019-01-01 00:53:20,1,1.50,1,N,151,239,1,7,0.5,0.5,1.65,0,0.3,9.95,
     */
    protected fun generateNYCTaxiData(index: String = "nyc-taxi-data") {
        createIndex(index, Settings.EMPTY, """"properties":{"DOLocationID":{"type":"integer"},"RatecodeID":{"type":"integer"},"fare_amount":{"type":"float"},"tpep_dropoff_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"congestion_surcharge":{"type":"float"},"VendorID":{"type":"integer"},"passenger_count":{"type":"integer"},"tolls_amount":{"type":"float"},"improvement_surcharge":{"type":"float"},"trip_distance":{"type":"float"},"store_and_fwd_flag":{"type":"keyword"},"payment_type":{"type":"integer"},"total_amount":{"type":"float"},"extra":{"type":"float"},"tip_amount":{"type":"float"},"mta_tax":{"type":"float"},"tpep_pickup_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"PULocationID":{"type":"integer"}}""")
        insertSampleBulkData(index, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())
    }

    companion object {
        internal interface IProxy {
            val version: String?
            var sessionId: String?

            fun getExecutionData(reset: Boolean): ByteArray?
            fun dump(reset: Boolean)
            fun reset()
        }

        /*
        * We need to be able to dump the jacoco coverage before the cluster shuts down.
        * The new internal testing framework removed some gradle tasks we were listening to,
        * to choose a good time to do it. This will dump the executionData to file after each test.
        * TODO: This is also currently just overwriting integTest.exec with the updated execData without
        *   resetting after writing each time. This can be improved to either write an exec file per test
        *   or by letting jacoco append to the file.
        * */
        @JvmStatic
        @AfterClass
        fun dumpCoverage() {
            // jacoco.dir set in esplugin-coverage.gradle, if it doesn't exist we don't
            // want to collect coverage, so we can return early
            val jacocoBuildPath = System.getProperty("jacoco.dir") ?: return
            val serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi"
            JMXConnectorFactory.connect(JMXServiceURL(serverUrl)).use { connector ->
                val proxy = MBeanServerInvocationHandler.newProxyInstance(
                    connector.mBeanServerConnection,
                    ObjectName("org.jacoco:type=Runtime"),
                    IProxy::class.java,
                    false
                )
                proxy.getExecutionData(false)?.let {
                    val path = Path.of("$jacocoBuildPath/integTest.exec")
                    Files.write(path, it)
                }
            }
        }
    }
}
