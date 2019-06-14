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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndexConfig
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class ManagedIndexConfigTests : ESTestCase() {

    fun `test managed index config parsing`() {

        val missingIndexUuid = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_name":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingIndex = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_name":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingName = """{"managed_index":{"enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_name":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingSchedule = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","last_updated_time":1560402722676,"enabled_time":null,"policy_name":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingLastUpdatedTime = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"enabled_time":null,"policy_name":"KumaJGCWPi","policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""
        val missingPolicyName = """{"managed_index":{"name":"edpNNwdVXG","enabled":false,"index":"DcdVHfmQUI","index_uuid":"SdcNvtdyAZYyrVkFMoQr","schedule":{"interval":{"start_time":1560402722674,"period":5,"unit":"Minutes"}},"last_updated_time":1560402722676,"enabled_time":null,"policy_seq_no":5,"policy_primary_term":17,"policy":{"name":"KumaJGCWPi","last_updated_time":1560402722676,"schema_version":348392,"default_notification":null,"default_state":"EpbLVqVhtL","states":[{"name":"EpbLVqVhtL","actions":[],"transitions":[]},{"name":"IIJxQdcenu","actions":[],"transitions":[]},{"name":"zSXlbLUBqG","actions":[],"transitions":[]},{"name":"nYRPBojBiy","actions":[],"transitions":[]}]},"change_policy":{"policy_name":"BtrDpcCBeT","state":"obxAkRuhvq"}}}"""

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing indexUuid") {
            ManagedIndexConfig.parseWithType(parserWithType(missingIndexUuid))
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing index") {
            ManagedIndexConfig.parseWithType(parserWithType(missingIndex))
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing name") {
            ManagedIndexConfig.parseWithType(parserWithType(missingName))
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing schedule") {
            ManagedIndexConfig.parseWithType(parserWithType(missingSchedule))
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing lastUpdatedTime") {
            ManagedIndexConfig.parseWithType(parserWithType(missingLastUpdatedTime))
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for missing policyName") {
            ManagedIndexConfig.parseWithType(parserWithType(missingPolicyName))
        }
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
