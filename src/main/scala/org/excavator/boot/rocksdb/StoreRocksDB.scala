package org.excavator.boot.rocksdb

import java.nio.file.Files

import org.rocksdb.util.SizeUnit
import org.rocksdb.{CompactionStyle, CompressionType, Options, RocksDB}
import org.slf4j.LoggerFactory

class StoreRocksDB {
  val logger = LoggerFactory.getLogger(classOf[StoreRocksDB])

  val encoding: String = "UTF-8"

  var isOpen = false

  private var rocksDB: RocksDB = null

  private def init(): Unit = {

    try {
      val dbFile = Files.createTempFile("rocksdb", ".db")

      val dbFileName = dbFile.getFileName.toUri.getPath

      RocksDB.loadLibrary()

      val options = new Options()

      options.setCreateIfMissing(true)
        .setWriteBufferSize(200 * SizeUnit.MB)
        .setMaxWriteBufferNumber(3)
        .setMaxBackgroundCompactions(10)
        .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .setCompactionStyle(CompactionStyle.UNIVERSAL)

      rocksDB = RocksDB.open(options, dbFileName)

      isOpen = true
    }catch {
      case ex: Throwable => {
        isOpen = false
        logger.error("init Exception = [{}]", ex.getMessage:Any, ex:Any)
      }
    }
  }

  init()
}

object StoreRocksDB{
  def apply(): StoreRocksDB = new StoreRocksDB()
}
