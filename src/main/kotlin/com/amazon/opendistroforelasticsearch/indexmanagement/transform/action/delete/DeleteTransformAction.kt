package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.delete.DeleteResponse

class DeleteTransformAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteTransformAction()
        val NAME = "cluster:admin/opendistro/transform/delete"
    }
}
