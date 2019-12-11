package org.excavator.boot.rocksdb

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.rocksdb.util.SizeUnit
import org.rocksdb.{CompactionStyle, CompressionType, Options, RocksDB, WriteBatch, WriteOptions}
import org.slf4j.LoggerFactory

class StoreRocksDB {
  val logger = LoggerFactory.getLogger(classOf[StoreRocksDB])

  val charset = StandardCharsets.UTF_8

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

  def put(k:String, v: String) = {
    assert(isOpen)

    rocksDB.put(k.getBytes(charset), v.getBytes(charset))
  }

  def putList(list: List[(String, String)]) = {
    assert(isOpen)
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()

    list.foreach(elem => {
      val (k, v) = elem
      batch.put(k.getBytes(charset), v.getBytes(charset))
    })

    rocksDB.write(writeOpt, batch)
  }

  def get(k:String): Option[String] = {
    assert(isOpen)
    try{
      val v = rocksDB.get(k.getBytes(charset))
      Option(new String(v, charset))
    }catch {
      case ex:Throwable => {
        logger.error("get Exception = [{}]", ex.getMessage:Any, ex:Any)
      }
    }
  }
}

object StoreRocksDB{
  def apply(): StoreRocksDB = new StoreRocksDB()
}
