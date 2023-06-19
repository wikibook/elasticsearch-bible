package kr.co.wikibook

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.ssl.SSLContexts
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

fun main() {
    val restClientBuilder = buildLowRestClient()
    val client = buildJavaClient(restClientBuilder)

    bulkIngesterExample(client)

    client._transport().close()
}

private fun buildLowRestClient(): RestClientBuilder {
    val restClientBuilder = RestClient.builder(
        HttpHost("hosts01", 9200, "https"),
        HttpHost("hosts02", 9200, "https"),
        HttpHost("hosts03", 9200, "https"),
    )

    restClientBuilder.setRequestConfigCallback {
        it.setConnectTimeout(5000)
            .setSocketTimeout(70000)
    }

    val trustStore = buildTrustStoreWithPem()
    //val trustStore = buildTrustStoreWithPkcs12()

    val sslBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null)
    val sslContext = sslBuilder.build()

    val credentialsProvider = BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials("my-username", "my-password321"))

    restClientBuilder.setHttpClientConfigCallback {
        it.setSSLContext(sslContext)
            .setDefaultCredentialsProvider(credentialsProvider)
    }

    return restClientBuilder
}

private fun buildTrustStoreWithPem(): KeyStore {
    val caPath = Paths.get("/path/to/http_ca.crt")
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val trustedCa = Files.newInputStream(caPath).use {
        certificateFactory.generateCertificate(it)
    }

    val trustStore = KeyStore.getInstance("pkcs12")
    trustStore.load(null, null)
    trustStore.setCertificateEntry("ca", trustedCa)

    return trustStore
}

private fun buildTrustStoreWithPkcs12(): KeyStore {
    val trustStorePath = Paths.get("/path/to/elastic-stack-ca.p12")
    val trustStore = KeyStore.getInstance("pkcs12")
    val trustStorePassword = "my-password123".toCharArray()

    Files.newInputStream(trustStorePath).use {
        trustStore.load(it, trustStorePassword)
    }

    return trustStore
}

private fun buildJavaClient(restClientBuilder: RestClientBuilder): ElasticsearchClient {
    val lowLevelRestClient = restClientBuilder.build()

    val mapper = jacksonMapperBuilder()
        .addModule(JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build()

    val transport = RestClientTransport(lowLevelRestClient, JacksonJsonpMapper(mapper))

    return ElasticsearchClient(transport)
}

private fun bulkIngesterExample(client: ElasticsearchClient) {
    val listener = BulkIngestListener<String>()

    val ingester = BulkIngester.of<String> {
        it.client(client)
            .maxOperations(200)
            .maxConcurrentRequests(1)
            .maxSize(5242880L) // 5MB
            .flushInterval(5L, TimeUnit.SECONDS)
            .listener(listener)
    }

    for (number in 0L until 1100L) {
        val bulkOperation = BulkOperation.of { bulkBuilder ->
            bulkBuilder.index { indexOpBuilder: IndexOperation.Builder<MyIndexClass> ->
                indexOpBuilder
                    .index("my-index")
                    .id("my-id-$number")
                    .routing("my-routing-$number")
                    .document(MyIndexClass("world", number, ZonedDateTime.now(UTC)))
            }
        }

        ingester.add(bulkOperation, "my-context-$number")
    }

    println("[${LocalDateTime.now()}] sleep 10 seconds ...")
    Thread.sleep(10000L)

    for (number in 1100L until 1200L) {
        val bulkOperation = BulkOperation.of { bulkBuilder ->
            bulkBuilder.index { indexOpBuilder: IndexOperation.Builder<MyIndexClass> ->
                indexOpBuilder
                    .index("my-index")
                    .id("my-id-$number")
                    .routing("my-routing-$number")
                    .document(MyIndexClass("world", number, ZonedDateTime.now(UTC)))
            }
        }

        ingester.add(bulkOperation, "my-context-$number")
    }

    println("[${LocalDateTime.now()}] sleep 10 seconds ...")
    Thread.sleep(10000L)

    ingester.close()
}