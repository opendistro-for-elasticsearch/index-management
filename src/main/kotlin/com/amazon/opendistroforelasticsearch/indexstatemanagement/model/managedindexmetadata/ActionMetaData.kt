package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData.Companion.NAME
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData.Companion.START_TIME
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

data class ActionMetaData(
    val name: String,
    val startTime: Long,
    val index: Int
) : Writeable, ToXContentFragment {
    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeLong(startTime)
        out.writeInt(index)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(NAME, name)
            .field(START_TIME, startTime)
            .field(INDEX, index)
    }

    fun getMapValueString(): String {
        return Strings.toString(this, false, false)
    }

    companion object {
        const val ACTION = "action"
        const val INDEX = "index"

        fun fromStreamInput(si: StreamInput): ActionMetaData {
            val name: String? = si.readString()
            val startTime: Long? = si.readLong()
            val index: Int? = si.readInt()

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(index) { "$INDEX is null" }
            )
        }

        fun fromManagedIndexMetaDataMap(map: Map<String, String?>): ActionMetaData? {
            val stateJsonString = map[ACTION]
            return if (stateJsonString != null) {
                val inputStream = ByteArrayInputStream(stateJsonString.toByteArray(StandardCharsets.UTF_8))
                val parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, inputStream)
                parser.nextToken()
                parse(parser)
            } else {
                null
            }
        }

        fun parse(xcp: XContentParser): ActionMetaData {
            var name: String? = null
            var startTime: Long? = null
            var index: Int? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME -> name = xcp.text()
                    START_TIME -> startTime = xcp.longValue()
                    INDEX -> index = xcp.intValue()
                }
            }

            return ActionMetaData(
                requireNotNull(name) { "$NAME is null" },
                requireNotNull(startTime) { "$START_TIME is null" },
                requireNotNull(index) { "$INDEX is null" }
            )
        }
    }
}
