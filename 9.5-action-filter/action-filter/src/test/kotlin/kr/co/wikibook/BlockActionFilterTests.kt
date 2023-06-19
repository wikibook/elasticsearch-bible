package kr.co.wikibook

import org.apache.http.util.EntityUtils
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.network.NetworkModule
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.test.ESIntegTestCase
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits
import org.elasticsearch.transport.netty4.Netty4Plugin
import org.junit.Before
import org.junit.Test

class BlockActionFilterTests : ESIntegTestCase() {
    override fun nodePlugins(): MutableCollection<Class<out Plugin>> {
        return mutableListOf(
            ActionFilterPlugin::class.java,
            Netty4Plugin::class.java
        )
    }

    override fun nodeSettings(nodeOrdinal: Int, otherSettings: Settings?): Settings {
        val defaultSettings = super.nodeSettings(nodeOrdinal, otherSettings)
        return Settings.builder()
            .put(defaultSettings)
            .put(NetworkModule.HTTP_TYPE_SETTING.key, "netty4")
            .putList("blocked.actions", listOf(
                "indices:data/write/delete",
                "indices:data/write/delete/byquery"
            ))
            .build()
    }

    override fun addMockHttpTransport(): Boolean {
        return false
    }

    @Before
    override fun setUp() {
        super.setUp()
        addDefaultData()
    }

    @Test
    fun testApprovedActions() {
        val queryBuilder = QueryBuilders.matchAllQuery()

        val searchSourceBuilder = SearchSourceBuilder()
            .from(0)
            .size(10)
            .query(queryBuilder)

        val searchRequest = SearchRequest()
            .indices("test-index")
            .source(searchSourceBuilder)

        val searchResponse = client().search(searchRequest).actionGet()
        for (hit in searchResponse.hits.hits) {
            logger.info("hit : {}", hit.sourceAsString)
        }

        assertHitCount(searchResponse, 3)
    }

    @Test(expected = ResponseException::class)
    fun testBlockedActions() {
        val restClient = getRestClient()
        try {
            restClient.performRequest(Request("DELETE", "test-index/_doc/1"))
        } catch (e: ResponseException) {
            val response = e.response

            val statusCode = response.statusLine.statusCode
            assertEquals(500, statusCode)

            val responseString = EntityUtils.toString(response.entity, Charsets.UTF_8)
            logger.info("responseString : {}", responseString)
            val expected = """{"error":{"root_cause":[{"type":"security_exception","reason":"unauthorized action"}],"type":"security_exception","reason":"unauthorized action"},"status":500}"""

            assertEquals(expected, responseString)

            throw e
        }
    }

    @Test(expected = ResponseException::class)
    fun testBlockedActions2() {
        val restClient = getRestClient()
        try {
            val request = Request("GET", "test-index/_search")
            val body = """
                {
                  "size": 0,
                  "aggs": {
                    "ag1": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      }
                    },
                    "ag2": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      }
                    },
                    "ag3": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      }
                    },
                    "ag4": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      }
                    },
                    "ag5": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      },
                      "aggs": {
                        "ag5-2": {
                          "terms": {
                            "field": "hello2",
                            "size": 10
                          }
                        }
                      }
                    }
                  }
                }
            """.trimIndent()
            request.setJsonEntity(body)
            restClient.performRequest(request)
        } catch (e: ResponseException) {
            val response = e.response

            val statusCode = response.statusLine.statusCode
            assertEquals(400, statusCode)

            val responseString = EntityUtils.toString(response.entity, Charsets.UTF_8)
            logger.info("responseString : {}", responseString)
            val expected = """{"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"too many aggregations"}],"type":"illegal_argument_exception","reason":"too many aggregations"},"status":400}"""

            assertEquals(expected, responseString)

            throw e
        }
    }

    @Test(expected = ResponseException::class)
    fun testBlockedActions3() {
        val restClient = getRestClient()
        try {
            val request = Request("GET", "test-index/_search")
            val body = """
                {
                  "size": 0,
                  "aggs": {
                    "ag1": {
                      "terms": {
                        "field": "hello",
                        "size": 10
                      },
                      "aggs": {
                        "ag2": {
                          "terms": {
                            "field": "hello2",
                            "size": 10
                          },
                          "aggs": {
                            "ag3": {
                              "terms": {
                                "field": "hello3",
                                "size": 10
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
            """.trimIndent()
            request.setJsonEntity(body)
            restClient.performRequest(request)
        } catch (e: ResponseException) {
            val response = e.response

            val statusCode = response.statusLine.statusCode
            assertEquals(400, statusCode)

            val responseString = EntityUtils.toString(response.entity, Charsets.UTF_8)
            logger.info("responseString : {}", responseString)
            val expected = """{"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"too deep aggregations"}],"type":"illegal_argument_exception","reason":"too deep aggregations"},"status":400}"""

            assertEquals(expected, responseString)

            throw e
        }
    }

    @Test
    fun testChangedActions() {
        val queryBuilder = QueryBuilders.termsQuery("hello", 7, 6, 5, 4, 3, 2, 1)

        val searchSourceBuilder = SearchSourceBuilder()
            .from(0)
            .size(10)
            .query(queryBuilder)

        val searchRequest = SearchRequest()
            .indices("test-index")
            .source(searchSourceBuilder)

        val searchResponse = client().search(searchRequest).actionGet()
        for (hit in searchResponse.hits.hits) {
            logger.info("hit : {}", hit.sourceAsString)
        }

        assertHitCount(searchResponse, 1)
        assertSearchHits(searchResponse, "3")
    }

    private fun addDefaultData() {
        client().prepareIndex()
            .setIndex("test-index")
            .setId("1")
            .setSource(mapOf("hello" to 1, "hello2" to 1, "hello3" to 1))
            .get()

        client().prepareIndex()
            .setIndex("test-index")
            .setId("2")
            .setSource(mapOf("hello" to 2, "hello2" to 2, "hello3" to 2))
            .get()

        client().prepareIndex()
            .setIndex("test-index")
            .setId("3")
            .setSource(mapOf("hello" to 3, "hello2" to 3, "hello3" to 3))
            .get()

        flushAndRefresh()
    }
}