package kr.co.wikibook.common

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.security.AccessController
import java.security.PrivilegedExceptionAction

val mapper = jacksonObjectMapper()

fun <T> doPrivileged(action: () -> T): T {
    return AccessController.doPrivileged(PrivilegedExceptionAction<T> {
        action.invoke()
    })
}

fun Any.toJson(): String {
    return doPrivileged { mapper.writeValueAsString(this) }
}

fun String.toMap(): Map<String, Any> {
    return mapper.readValue(this)
}