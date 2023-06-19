package kr.co.wikibook

import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.ssl.SSLContexts
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.*
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.CertificateFactory

fun main() {
    val lowLevelClient = buildClient()
    //val lowLevelClient = buildClientWithPem()
    //val lowLevelClient = buildClientWithPkcs12()

    lowLevelClientSample(lowLevelClient)
    lowLevelClient.close()
}

fun buildClient(): RestClient {
    val restClientBuilder = RestClient.builder(
        HttpHost("hosts01", 9200, "http"),
        HttpHost("hosts02", 9200, "http"),
        HttpHost("hosts03", 9200, "http"),
    )

    restClientBuilder.setDefaultHeaders(
        arrayOf<Header>(BasicHeader("my-header", "my-value"))
    )

    restClientBuilder.setRequestConfigCallback {
        it.setConnectTimeout(5000)
            .setSocketTimeout(70000)
    }

    val lowLevelClient = restClientBuilder.build()

    return lowLevelClient
}

fun buildClientWithPem(): RestClient {
    val restClientBuilder = RestClient.builder(
        HttpHost("hosts01", 9200, "https"),
        HttpHost("hosts02", 9200, "https"),
        HttpHost("hosts03", 9200, "https"),
    )

    restClientBuilder.setDefaultHeaders(
        arrayOf<Header>(BasicHeader("my-header", "my-value"))
    )

    restClientBuilder.setRequestConfigCallback {
        it.setConnectTimeout(5000)
            .setSocketTimeout(70000)
    }

    val caPath = Paths.get("/path/to/http_ca.crt")
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val trustedCa = Files.newInputStream(caPath).use {
        certificateFactory.generateCertificate(it)
    }

    val trustStore = KeyStore.getInstance("pkcs12")
    trustStore.load(null, null)
    trustStore.setCertificateEntry("ca", trustedCa)

    val sslBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null)
    val sslContext = sslBuilder.build()

    val credentialsProvider = BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials("my-username", "my-password321"))

    restClientBuilder.setHttpClientConfigCallback {
        it.setSSLContext(sslContext)
            .setDefaultCredentialsProvider(credentialsProvider)
    }

    val lowLevelClient = restClientBuilder.build()

    return lowLevelClient
}

fun buildClientWithPkcs12(): RestClient {
    val restClientBuilder = RestClient.builder(
        HttpHost("hosts01", 9200, "https"),
        HttpHost("hosts02", 9200, "https"),
        HttpHost("hosts03", 9200, "https"),
    )

    restClientBuilder.setDefaultHeaders(
        arrayOf<Header>(BasicHeader("my-header", "my-value"))
    )

    restClientBuilder.setRequestConfigCallback {
        it.setConnectTimeout(5000)
            .setSocketTimeout(70000)
    }

    val trustStorePath = Paths.get("/path/to/elastic-stack-ca.p12")
    val trustStore = KeyStore.getInstance("pkcs12")
    val trustStorePassword = "my-password123".toCharArray()
    Files.newInputStream(trustStorePath).use {
        trustStore.load(it, trustStorePassword)
    }

    val sslBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null)
    val sslContext = sslBuilder.build()

    val credentialsProvider = BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials("my-username", "my-password321"))

    restClientBuilder.setHttpClientConfigCallback {
        it.setSSLContext(sslContext)
            .setDefaultCredentialsProvider(credentialsProvider)
    }

    val lowLevelClient = restClientBuilder.build()

    return lowLevelClient
}

fun lowLevelClientSample(lowLevelClient: RestClient) {
    // sync request
    val getSettingsRequest = Request("GET", "/_cluster/settings")
    getSettingsRequest.addParameters(mutableMapOf("pretty" to "true"))

    val getSettingsResponse: Response = lowLevelClient.performRequest(getSettingsRequest)
    printResponse(getSettingsResponse)

    // async request
    val updateSettingsRequest = Request("PUT", "/_cluster/settings")
    val requestBody = """
        {
            "transient": {
                "cluster.routing.allocation.enable": "all"
            }
        }
    """.trimIndent()
    updateSettingsRequest.entity = NStringEntity(requestBody, ContentType.APPLICATION_JSON)

    val cancellable: Cancellable = lowLevelClient.performRequestAsync(updateSettingsRequest,
        object : ResponseListener {
            override fun onSuccess(response: Response) {
                printResponse(response)
            }

            override fun onFailure(exception: Exception) {
                System.err.println(exception)
            }
        })

    // 필요하다면 요청의 취소가 가능
    Thread.sleep(1000L)
    cancellable.cancel()
}

fun printResponse(response: Response) {
    val statusCode = response.statusLine.statusCode
    val responseBody = EntityUtils.toString(response.entity, Charsets.UTF_8)
    println("statusCode : $statusCode, responseBody : $responseBody")
}