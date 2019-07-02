package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexstatemanagement.action.OpenAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import java.io.IOException

class OpenActionConfig(
    val timeout: ActionTimeout?,
    val retry: ActionRetry?,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.OPEN, timeout, retry, index) {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startObject(ActionType.OPEN.type)
        super.toXContent(builder, params)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = OpenAction(clusterService, client, managedIndexMetaData, this)

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): OpenActionConfig {
            var timeout: ActionTimeout? = null
            var retry: ActionRetry? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                    ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in OpenActionConfig.")
                }
            }

            return OpenActionConfig(timeout, retry, index)
        }
    }
}
