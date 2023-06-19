package kr.co.wikibook.service

data class CdcMessage(
    val operation: String,
    val index: String,
    val shard: Int,
    val id: String,
    val routing: String?,
    val before: Map<String, Any?>?,
    val after: Map<String, Any?>?
)