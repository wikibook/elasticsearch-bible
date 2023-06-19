package kr.co.wikibook.service

import kr.co.wikibook.service.DumpService.DumpType.FS
import kr.co.wikibook.service.DumpService.DumpType.HDFS
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings

class DumpService @Inject constructor(
    settings: Settings
) : AbstractLifecycleComponent() {
    private val writer: DumpWriter

    init {
        val dumpType = DumpType.valueOf(settings.get("dump.type").uppercase())
        when (dumpType) {
            FS -> this.writer = LocalFileWriter()

            HDFS -> {
                val configs = settings.getAsList("dump.hdfs.configs")
                val defaultFs = settings.get("dump.hdfs.defaultFs")
                val userName = settings.get("dump.hdfs.userName")

                this.writer = HdfsWriter(configs, defaultFs, userName)
            }
        }
    }

    fun writeLines(fileName: String, lines: List<String>, append: Boolean): Long {
        return writer.writeLines(fileName, lines, append)
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
    }

    enum class DumpType {
        FS, HDFS
    }
}