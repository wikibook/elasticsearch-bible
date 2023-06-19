package kr.co.wikibook

import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.message.BasicHeader
import org.apache.http.ssl.SSLContexts
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.*
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.core.TimeValue
import org.elasticsearch.index.query.*
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.xcontent.XContentType
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.CertificateFactory

fun main() {
    //val restClientBuilder = getLowLevelClientBuilder()
    val restClientBuilder = buildClientWithPem()

    val highLevelClient = RestHighLevelClientBuilder(restClientBuilder.build())
        .setApiCompatibilityMode(true)
        .build()

    getSample(highLevelClient)
    searchSample(highLevelClient)
    bulkSample(highLevelClient)

    val bulkProcessor = buildBulkProcessor(highLevelClient)
    bulkProcessorSample(bulkProcessor)

    Thread.sleep(10000L)
    highLevelClient.close()
}

fun buildClientWithPem(): RestClientBuilder {
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

    return restClientBuilder
}

fun getLowLevelClientBuilder(): RestClientBuilder {
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

    return restClientBuilder
}

fun searchSample(highLevelClient: RestHighLevelClient) {
    val queryBuilder = QueryBuilders.boolQuery()
        .must(TermQueryBuilder("fieldOne", "hello"))
        .should(MatchQueryBuilder("fieldTwo", "hello world").operator(Operator.AND))
        .should(RangeQueryBuilder("fieldThree").gte(100).lt(200))
        .minimumShouldMatch(1)

    val searchSourceBuilder = SearchSourceBuilder()
        .from(0)
        .size(10)
        .query(queryBuilder)

    val searchRequest = SearchRequest()
        .indices("my-index-01", "my-index-02")
        .routing("abc123")
        .source(searchSourceBuilder)

    val searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT)
    val searchHits = searchResponse.hits
    val totalHits = searchHits.totalHits
    println("totalHits : $totalHits")
    println(searchHits.hits.map { it.sourceAsMap })
}

fun getSample(highLevelClient: RestHighLevelClient) {
    val getRequest = GetRequest()
        .index("my-index-01")
        .id("document-id-01")
        .routing("abc123")

    val getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT)
    println(getResponse.sourceAsMap)
}

fun buildBulkProcessor(client: RestHighLevelClient): BulkProcessor {
    val bulkAsync = { request: BulkRequest, listener: ActionListener<BulkResponse> ->
        client.bulkAsync(request, RequestOptions.DEFAULT, listener)
        Unit
    }

    return BulkProcessor.builder(bulkAsync, EsBulkListener(), "myBulkProcessorName")
        .setBulkActions(50000)
        .setBulkSize(ByteSizeValue.ofMb(50L))
        .setFlushInterval(TimeValue.timeValueMillis(5000L))
        .setConcurrentRequests(1)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff())
        .build()
}

fun bulkSample(highLevelClient: RestHighLevelClient) {
    val bulkRequest = BulkRequest()
    bulkRequest.add(
        UpdateRequest()
            .index("my-index-01")
            .id("document-id-01")
            .routing("abc123")
            .doc(mapOf("hello" to "elasticsearch"))
    )

    val bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    if (bulkResponse.hasFailures()) {
        System.err.println(bulkResponse.buildFailureMessage())
    }
}

fun bulkProcessorSample(bulkProcessor: BulkProcessor) {
    val source = mapOf<String, Any>(
        "hello" to "world",
        "world" to 123
    )

    val indexRequest = IndexRequest("my-index-01")
        .id("document-id-01")
        .routing("abc123")
        .source(source, XContentType.JSON)

    bulkProcessor.add(indexRequest)
}