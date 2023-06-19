package kr.co.wikibook

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.GetRequest
import co.elastic.clients.elasticsearch.core.IndexRequest
import co.elastic.clients.elasticsearch.core.bulk.*
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
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.RestHighLevelClientBuilder
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.ZoneOffset
import java.time.ZonedDateTime

fun main() {
    val restClientBuilder = buildLowRestClient()
    val client = buildJavaClient(restClientBuilder)

    indexExample(client)
    getExample(client)

    bulkExampleOne(client)
    bulkExampleTwo(client)

    searchExample(client)

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

private fun buildHlrcAndJavaClient(restClientBuilder: RestClientBuilder): Pair<RestHighLevelClient, ElasticsearchClient> {
    val hlrc = RestHighLevelClientBuilder(restClientBuilder.build())
        .setApiCompatibilityMode(true)
        .build()

    val mapper = jacksonMapperBuilder()
        .addModule(JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build()

    val lowLevelRestClient = hlrc.lowLevelClient
    val transport = RestClientTransport(lowLevelRestClient, JacksonJsonpMapper(mapper))
    val client = ElasticsearchClient(transport)

    return hlrc to client
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

private fun indexExample(client: ElasticsearchClient) {
    val indexResponse = client.index { builder: IndexRequest.Builder<MyIndexClass> ->builder
        .index("my-index")
        .id("my-id-1")
        .routing("my-routing-1")
        .document(MyIndexClass("hello", 1L, ZonedDateTime.now(ZoneOffset.UTC)))
    }

    val result = indexResponse.result()
    println(result.name)
}

private fun getExample(client: ElasticsearchClient) {
    val getRequest = GetRequest.Builder()
        .index("my-index")
        .id("my-id-1")
        .routing("my-routing-1")
        .build()

    val response = client.get(getRequest, MyIndexClass::class.java)
    println(response.source())
}

private fun bulkExampleOne(client: ElasticsearchClient) {
    val createOperation = CreateOperation.Builder<MyIndexClass>()
        .index("my-index")
        .id("my-id-2")
        .routing("my-routing-2")
        .document(MyIndexClass("world", 2L, ZonedDateTime.now(ZoneOffset.UTC)))
        .build()

    val indexOperation = IndexOperation.Builder<MyIndexClass>()
        .index("my-index")
        .id("my-id-3")
        .routing("my-routing-3")
        .document(MyIndexClass("world", 4L, ZonedDateTime.now(ZoneOffset.UTC)))
        .build()

    val updateAction = UpdateAction.Builder<MyIndexClass, MyPartialIndexClass>()
        .doc(MyPartialIndexClass("world updated"))
        .build()

    val updateOperation = UpdateOperation.Builder<MyIndexClass, MyPartialIndexClass>()
        .index("my-index")
        .id("my-id-1")
        .routing("my-routing-1")
        .action(updateAction)
        .build()

    val bulkOpOne = BulkOperation.Builder().create(createOperation).build()
    val bulkOpTwo = BulkOperation.Builder().index(indexOperation).build()
    val bulkOpThree = BulkOperation.Builder().update(updateOperation).build()

    val operations = listOf<BulkOperation>(bulkOpOne, bulkOpTwo, bulkOpThree)
    val bulkResponse = client.bulk { it.operations(operations) }

    for (item in bulkResponse.items()) {
        println("result : ${item.result()}, error : ${item.error()}")
    }
}

private fun bulkExampleTwo(client: ElasticsearchClient) {
    val bulkResponse = client.bulk { _0 -> _0
        .operations { _1 -> _1
            .index { _2: IndexOperation.Builder<MyIndexClass> -> _2
                .index("my-index")
                .id("my-id-4")
                .routing("my-routing-4")
                .document(MyIndexClass("world", 4L, ZonedDateTime.now(ZoneOffset.UTC)))
            }
        }
        .operations { _1 -> _1
            .update { _2: UpdateOperation.Builder<MyIndexClass, MyPartialIndexClass> -> _2
                .index("my-index")
                .id("my-id-2")
                .routing("my-routing-2")
                .action { _3 -> _3
                    .doc(MyPartialIndexClass("world updated"))
                }
            }
        }
    }

    for (item in bulkResponse.items()) {
        println("result : ${item.result()}, error : ${item.error()}")
    }
}

private fun searchExample(client: ElasticsearchClient) {
    val response = client.search({ builder -> builder
        .index("my-index")
        .from(0)
        .size(10)
        .query { query -> query
            .term { term -> term
                .field("fieldOne")
                .value { value -> value
                    .stringValue("world")
                }
            }
        }
    }, MyIndexClass::class.java)

    for (hit in response.hits().hits()) {
        println(hit.source())
    }
}
