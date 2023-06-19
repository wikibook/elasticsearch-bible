package kr.co.wikibook.service

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.logging.log4j.LogManager
import java.lang.Exception

class KafkaProduceCallback(private val message: CdcMessage) : Callback {
    private val log = LogManager.getLogger(javaClass)

    override fun onCompletion(metadata: RecordMetadata?, e: Exception?) {
        if (e == null) {
            return
        }

        log.error(e.toString(), e)
        log.error("kafka publish failed. message : {}, metadata : {}", message, metadata)
    }
}