package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.bulk.BulkResponse

class DeleteTransformsAction private constructor() : ActionType<BulkResponse>(NAME, ::BulkResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
