package kr.co.wikibook

import com.carrotsearch.randomizedtesting.annotations.Seed
import org.elasticsearch.test.ESTestCase
import org.hamcrest.Matchers.greaterThan
import org.junit.Test

//@Seed("BE3FB3441F898691") // if needed
class SmallTests: ESTestCase() {
    @Test
    fun testBasics() {
        logger.info("SmallTests testBasics")
        assertEquals(1L, 1L)
        //assertEquals(1L, randomLongBetween(2L, 10L))
        assertThat(randomNonNegativeLong(), greaterThan(0L))
    }
}