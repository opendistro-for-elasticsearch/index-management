package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import org.elasticsearch.rest.RestStatus
import java.util.*

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {

    fun `test ISM template`() {
        val templateName = "t1"
        println("template name $templateName")
        val ismTemplate = """
            {
                "index_patterns": ["log*"],
                "policy_id": "policy_1",
                "priority": 100
            }
        """.trimIndent()
        var res = createISMTemplateJson(templateName, ismTemplate)
        assertEquals("Unable to create new ISM template", RestStatus.CREATED, res.restStatus())

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

        res = createISMTemplateJson(templateName, ismTemplate)
        assertEquals("Unable to update new ISM template", RestStatus.OK, res.restStatus())


        val ismTemplatesRes = getISMTemplates(templateName)
        println("get template parsed response $ismTemplatesRes")

        // createISMTemplateJson("t2", """
        //     {
        //         "index_patterns": ["log*"],
        //         "policy_id": "policy_1",
        //         "priority": 50
        //     }
        // """.trimIndent())

        val mapp1 = getISMTemplatesMap(templateName)
        println("get template response map $mapp1")
        assertPredicatesOnISMTemplates(
            listOf(
                templateName to listOf(
                    "template_name" to templateName::equals,
                    "ism_template" to fun(template: Any?): Boolean = assertISMTemplateEquals(
                        ISMTemplate(listOf("log*"), "policy_1", 100, randomInstant()),
                        template
                    )
                )
            ),
            mapp1
        )
    }
}