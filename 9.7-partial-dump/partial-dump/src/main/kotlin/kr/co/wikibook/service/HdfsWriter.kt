package kr.co.wikibook.service

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.LogManager
import java.net.URI
import java.net.URL
import java.security.PrivilegedExceptionAction
import java.util.zip.GZIPOutputStream

class HdfsWriter(
    private val configs: List<String>,
    private val defaultFs: String,
    userName: String
) : DumpWriter {
    private val ugi: UserGroupInformation = UserGroupInformation.createRemoteUser(userName)
    private val lineSeparator = System.lineSeparator().toByteArray()

    override fun writeLines(fileName: String, lines: List<String>, append: Boolean): Long {
        var writeCount = 0L
        val fs = initFs()
        fs.use {
            val gzipOutputStream = GZIPOutputStream(makeOutputStream(fs, fileName, append))
            gzipOutputStream.use { out ->
                for (line in lines) {
                    out.write(line.toByteArray())
                    out.write(lineSeparator)
                    writeCount++
                }
            }
        }

        return writeCount
    }

    private fun initFs(): DistributedFileSystem {
        return ugi.doAs(PrivilegedExceptionAction {
            for (config in configs) {
                val fs = initFsFromConfigUrl(config, defaultFs)
                if (fs != null) {
                    return@PrivilegedExceptionAction fs
                }
            }

            throw IllegalArgumentException("initFs failed with given configs")
        })
    }

    private fun initFsFromConfigUrl(url: String, defaultFs: String): DistributedFileSystem? {
        val originalClassLoader = Thread.currentThread().contextClassLoader
        Thread.currentThread().contextClassLoader = null

        val conf = Configuration()
        conf.addResource(URL(url))
        conf["fs.hdfs.impl"] = DistributedFileSystem::class.java.name
        conf["fs.file.impl"] = LocalFileSystem::class.java.name

        val fileSystem = if (defaultFs.isBlank()) {
            FileSystem.newInstance(conf)
        } else {
            FileSystem.newInstance(URI(defaultFs), conf)
        }
        Thread.currentThread().contextClassLoader = originalClassLoader

        return fileSystem as DistributedFileSystem?
    }

    private fun makeOutputStream(fs: DistributedFileSystem, fileName: String, append: Boolean = true): FSDataOutputStream {
        val outputPath = Path(fileName)
        require(!fs.isDirectory(outputPath)) { "{$outputPath.name} is a directory" }

        if (!fs.exists(outputPath)) {
            return fs.create(outputPath)
        }

        if (!fs.isFileClosed(outputPath)) {
            fs.recoverLease(outputPath)
            runBlocking {
                withTimeout(30000L) {
                    while (!fs.isFileClosed(outputPath)) {
                        delay(100L)
                    }
                }
            }
        }

        return if (append) {
            fs.append(outputPath)
        } else {
            fs.create(outputPath)
        }
    }
}