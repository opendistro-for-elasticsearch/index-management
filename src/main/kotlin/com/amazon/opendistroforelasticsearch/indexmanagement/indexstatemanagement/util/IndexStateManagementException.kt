package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchSecurityException
import org.elasticsearch.rest.RestStatus

private val log = LogManager.getLogger(IndexStateManagementException::class.java)

class IndexStateManagementException(message: String, val status: RestStatus, ex: Exception) : ElasticsearchException(message, ex) {
    companion object {
        @JvmStatic
        fun wrap(ex: Exception): ElasticsearchException {
            log.info("Index State Management Plugin error: $ex")

            var friendlyMsg = "Unexpected error in ISM: ${ex.message}"
            var status = RestStatus.INTERNAL_SERVER_ERROR
            when (ex) {
                is ElasticsearchSecurityException -> {
                    status = ex.status()
                    friendlyMsg = "User doesn't have permissions to execute this action. Contact administrator."
                }
            }
            return IndexStateManagementException(friendlyMsg, status, Exception("${ex.javaClass.name}: ${ex.message}"))
        }
    }
}
