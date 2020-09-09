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

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementRestTestCase
import org.elasticsearch.client.Request
import org.elasticsearch.common.io.Streams
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class RefreshSynonymAnalyzerActionIT : IndexManagementRestTestCase() {
    fun `test index time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getIndexAnalyzerSettings(), XContentType.JSON)
                .build()
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)
        Thread.sleep(1000) // wait for refresh_interval

        val result1 = queryData(indexName, "hello")
        assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should NOT match
        val result5 = queryData(indexName, "namaste")
        assertFalse(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    fun `test search time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
                .build()
        // val mappings: String = "\"properties\":{\"title\":{\"type\": \"text\",\"analyzer\" : \"standard\",\"search_analyzer\": \"my_synonyms\"}}"
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)
        Thread.sleep(1000) // wait for refresh_interval

        val result1 = queryData(indexName, "hello")
        assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should match
        val result5 = queryData(indexName, "namaste")
        assertTrue(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    fun `test alias`() {
        val indexName = "testindex"
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val buildDir = System.getProperty("buildDir")
        val aliasName = "test"
        val aliasSettings = "\"$aliasName\": {}"

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola")
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
                .build()
        createIndex(indexName, settings, getAnalyzerMapping(), aliasSettings)
        ingestData(indexName)
        Thread.sleep(1000)

        val result1 = queryData(aliasName, "hello")
        assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(aliasName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(aliasName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            writeToFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt", "hello, hola, namaste")
        }

        // New added synonym should NOT match
        val result4 = queryData(aliasName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(aliasName)

        // New added synonym should match
        val result5 = queryData(aliasName, "namaste")
        assertTrue(result5.contains("hello world"))

        for (i in 0 until numNodes) {
            deleteFile("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
        }
    }

    companion object {

        fun writeToFile(filePath: String, contents: String) {
            var path = org.elasticsearch.common.io.PathUtils.get(filePath)
            Files.newBufferedWriter(path, Charset.forName("UTF-8")).use { writer -> writer.write(contents) }
        }

        fun deleteFile(filePath: String) {
            // org.elasticsearch.common.io.PathUtils.get(filePath)
            Files.deleteIfExists(org.elasticsearch.common.io.PathUtils.get(filePath))
        }

        fun ingestData(indexName: String) {
            val request = Request("POST", "/$indexName/_doc")
            val data: String = """
                {
                  "title": "hello world..."
                }
            """.trimIndent()
            request.setJsonEntity(data)
            client().performRequest(request)
        }

        fun queryData(indexName: String, query: String): String {
            val request = Request("GET", "/$indexName/_search?q=$query")
            val response = client().performRequest(request)
            return Streams.copyToString(InputStreamReader(response.entity.content, StandardCharsets.UTF_8))
        }

        fun refreshAnalyzer(indexName: String) {
            val request = Request("POST",
                    "${IndexManagementPlugin.ANALYZER_BASE_URI}/refresh_synonym_analyzer/$indexName")
            client().performRequest(request)
        }

        fun getSearchAnalyzerSettings(): String {
            return """
            {
                "index" : {
                    "analysis" : {
                        "analyzer" : {
                            "my_synonyms" : {
                                "tokenizer" : "whitespace",
                                "filter" : ["synonym"]
                            }
                        },
                        "filter" : {
                            "synonym" : {
                                "type" : "synonym_graph",
                                "synonyms_path" : "pacman_synonyms.txt", 
                                "updateable" : true 
                            }
                        }
                    }
                }
            }
            """.trimIndent()
        }

        fun getIndexAnalyzerSettings(): String {
            return """
            {
                "index" : {
                    "analysis" : {
                        "analyzer" : {
                            "my_synonyms" : {
                                "tokenizer" : "whitespace",
                                "filter" : ["synonym"]
                            }
                        },
                        "filter" : {
                            "synonym" : {
                                "type" : "synonym_graph",
                                "synonyms_path" : "pacman_synonyms.txt"
                            }
                        }
                    }
                }
            }
            """.trimIndent()
        }

        fun getAnalyzerMapping(): String {
            return """
            "properties": {
                    "title": {
                        "type": "text",
                        "analyzer" : "standard",
                        "search_analyzer": "my_synonyms"
                    }
                }
            """.trimIndent()
        }
    }
}
