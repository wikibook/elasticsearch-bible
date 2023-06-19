package kr.co.wikibook.service

interface CdcPublisher {
    fun publish(message: CdcMessage)
    fun close()
}