package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.elasticsearch.action.ActionType

class RetryFailedManagedIndexAction private constructor() : ActionType<RetryFailedManagedIndexResponse>(NAME, ::RetryFailedManagedIndexResponse) {
    companion object {
        val INSTANCE = RetryFailedManagedIndexAction()
        val NAME = "cluster:admin/ism/managedindex/retry"
    }
}
