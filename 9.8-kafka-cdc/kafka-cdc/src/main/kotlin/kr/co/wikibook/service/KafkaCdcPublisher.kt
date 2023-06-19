package kr.co.wikibook.service

import kr.co.wikibook.common.doPrivileged
import kr.co.wikibook.common.toJson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.common.settings.Settings
import java.util.*

class KafkaCdcPublisher(settings: Settings) : CdcPublisher {
    private val topicSuffix: String
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties().also { properties ->

            properties[BOOTSTRAP_SERVERS_CONFIG] = settings.get("cdc.kafka.brokers")
            properties[KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
            properties[VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName

            val kafkaSettings = settings.getAsSettings("cdc.kafka.settings")
            kafkaSettings.names().forEach { name ->
                properties[name] = kafkaSettings.get(name)
            }
        }

        this.topicSuffix = settings.get("cdc.kafka.topicSuffix")

        this.producer = doPrivileged {
            val originalClassLoader = Thread.currentThread().contextClassLoader
            Thread.currentThread().contextClassLoader = null
            val kafkaProducer = KafkaProducer<String, String>(props)
            Thread.currentThread().contextClassLoader = originalClassLoader
            kafkaProducer
        }
    }

    override fun publish(message: CdcMessage) {
        val record = ProducerRecord(buildTopic(message), buildKey(message), buildMessage(message))
        val callback = KafkaProduceCallback(message)

        doPrivileged { producer.send(record, callback) }
    }

    private fun buildTopic(message: CdcMessage): String {
        return message.index + topicSuffix
    }

    private fun buildKey(message: CdcMessage): String {
        // kafka의 key값은 문서의 _routing이 아니라 shard number를 활용한다.
        return message.shard.toString()
    }

    private fun buildMessage(message: CdcMessage): String {
        return message.toJson()
    }

    override fun close() {
        producer.close()
    }
}