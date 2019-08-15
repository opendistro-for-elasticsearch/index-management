/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata

import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken

/** Properties that will persist across steps of a single Action. Will be stored in the [ActionMetaData]. */
class ActionProperties : Writeable, ToXContentFragment {

    private val properties: MutableMap<String, Any> = mutableMapOf()

    override fun writeTo(out: StreamOutput) {
        out.writeMap(properties)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.map(properties)
    }

    fun get(property: String): Any? {
        require(properties.containsKey(property)) { "No property [$property] in ActionProperties" }

        return properties[property]
    }

    /*
     * TODO: Creating methods to return specific types as there aren't many properties at the moment
     *  This can later be removed or extended to support returning multiple types without requiring unchecked casts from the caller
     */
    fun getBoolean(property: String): Boolean {
        require(properties.containsKey(property)) { "No property [$property] in ActionProperties" }

        val propertyValue: Any? = properties[property]
        require(propertyValue is Boolean) { "Property [$property]'s value [$propertyValue] is not a Boolean" }

        return propertyValue
    }

    fun getInt(property: String): Int {
        require(properties.containsKey(property)) { "No property [$property] in ActionProperties" }

        val propertyValue: Any? = properties[property]
        require(propertyValue is Int) { "Property [$property]'s value [$propertyValue] is not an Integer" }

        return propertyValue
    }

    fun put(property: String, value: Any): ActionProperties {
        require(supportedProperties.contains(property)) { "[$property] is not a supported Action property" }

        properties[property] = value
        return this
    }

    override fun equals(other: Any?): Boolean =
        if (other is ActionProperties) {
            this.properties == other.properties
        } else {
            false
        }

    override fun hashCode(): Int = properties.hashCode()

    companion object {
        const val ACTION_PROPERTIES = "action_properties"
        const val WAS_READ_ONLY = "was_read_only"
        const val MAX_NUM_SEGMENTS = "max_num_segments"

        val supportedProperties = setOf(
            WAS_READ_ONLY,
            MAX_NUM_SEGMENTS
        )

        fun fromMap(properties: Map<String, Any>): ActionProperties {
            val actionProperties = ActionProperties()

            for ((k, v) in properties) {
                actionProperties.put(k, v)
            }

            return actionProperties
        }

        fun fromStreamInput(si: StreamInput): ActionProperties {
            val properties: Map<String, Any>? = si.readMap()

            requireNotNull(properties) { "Properties are null" }

            return ActionProperties.fromMap(properties)
        }

        fun parse(xcp: XContentParser): ActionProperties {

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            val properties: MutableMap<String, Any> = xcp.map()

            return ActionProperties.fromMap(properties)
        }
    }
}
