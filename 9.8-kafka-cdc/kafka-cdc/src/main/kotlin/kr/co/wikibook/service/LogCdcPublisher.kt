package kr.co.wikibook.service

import kr.co.wikibook.common.toJson
import org.apache.logging.log4j.LogManager

class LogCdcPublisher : CdcPublisher {
    private val log = LogManager.getLogger(javaClass)

    override fun publish(message: CdcMessage) {
        log.info(message.toJson())
    }

    override fun close() {
    }
}