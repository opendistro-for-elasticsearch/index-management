package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.elasticsearch.action.ActionType

class ChangePolicyAction private constructor() : ActionType<ChangePolicyResponse>(NAME, ::ChangePolicyResponse) {
    companion object {
        val INSTANCE = ChangePolicyAction()
        val NAME = "cluster:admin/ism/managedindex/change"
    }
}
