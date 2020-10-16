package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.Action
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action.RollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.script.ScriptService
import java.io.IOException

// TODO: Not sure what interface should be
class RollupActionConfig(
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ROLLUP, index) {

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = RollupAction(clusterService, client, managedIndexMetaData, this)

    override fun isFragment(): Boolean {
        TODO("Not yet implemented")
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): RollupActionConfig {

            // TODO: implement logic to parse the xcp
            return RollupActionConfig(
                    index = index
            )
        }
    }
}
