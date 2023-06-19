package kr.co.wikibook.service

interface DumpWriter {
    fun writeLines(fileName: String, lines: List<String>, append: Boolean): Long
}