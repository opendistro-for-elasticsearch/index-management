package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.delete.DeleteResponse

class DeleteTransformsAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteTransformsAction()
        val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
