package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.elasticsearch.common.io.Streams
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.json.JsonXContent
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {
    fun `test create ISM template`() {
        val ismTemplate = """
            {
                "index_patterns": ["log*"],
                "policy_id": "policy_1",
                "priority": 100
            }
        """.trimIndent()
        val res = createISMTemplateJson("t1", ismTemplate)
        // val str1 = res.entity.content.bufferedReader().readText()
        // val str2 = Streams.copyToString(InputStreamReader(res.entity.content))
        // val map1 = JsonXContent.jsonXContent
        //     .createParser(NamedXContentRegistry.EMPTY,
        //         LoggingDeprecationHandler.INSTANCE,
        //         res.entity.content).map()

        println("create template response $res")
        // println("create template response string1 $str1")
        // println("create template response string2 $str2")
        // println("create template response map1 $map1")

        val ismTemplatesRes = getISMTemplate("t1")
        println("get template parsed response $ismTemplatesRes")
    }

    fun `test get ISM template`() {

    }

    fun `test delete ISM template`() {

    }
}