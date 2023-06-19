package kr.co.wikibook.service

import java.io.File
import java.io.FileOutputStream
import java.security.AccessController
import java.security.PrivilegedExceptionAction

class LocalFileWriter : DumpWriter {
    override fun writeLines(fileName: String, lines: List<String>, append: Boolean): Long {
        var writeCount = 0L
        AccessController.doPrivileged(PrivilegedExceptionAction {
            FileOutputStream(File(fileName), append)
                .bufferedWriter()
                .use { out ->
                    lines.forEach { line ->
                        out.write(line)
                        out.newLine()
                        writeCount++
                    }
                }
        })
        return writeCount
    }
}