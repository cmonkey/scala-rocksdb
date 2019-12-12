package org.excavator.boot.rocksdb

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.rocksdb.util.SizeUnit
import org.rocksdb.{CompactionStyle, CompressionType, Options, RocksDB, WriteBatch, WriteOptions}
import org.slf4j.LoggerFactory

class StoreRocksDB(path: String) {
  val logger = LoggerFactory.getLogger(classOf[StoreRocksDB])

  val charset = StandardCharsets.UTF_8

  var isOpen = false

  private var rocksDB: RocksDB = null

  private def init(): Unit = {

    try {
      logger.info("init RocksDB path  = [{}]", path)

      RocksDB.loadLibrary()

      val options = new Options()

      options.setCreateIfMissing(true)
        .setWriteBufferSize(200 * SizeUnit.MB)
        .setMaxWriteBufferNumber(3)
        .setMaxBackgroundCompactions(10)
        .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .setCompactionStyle(CompactionStyle.UNIVERSAL)

      rocksDB = RocksDB.open(options, path)

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
        None
      }
    }
  }

  def delete(k: String): Option[Boolean] = {
    assert(isOpen)
    try{
      rocksDB.delete(k.getBytes(charset))
      Option(true)
    }catch{
      case ex: Throwable => {
        logger.error("delete Exception = [{}]", ex.getMessage(): Any, ex:Any)
        None
      }
    }
  }

  def shutdown(): Unit = {
    try {
      rocksDB.close()
      isOpen = false
      logger.info("shutdown RocksDB")
    }catch {
      case ex:Throwable => {
        logger.error("shutdown failed Exception = [{}]", ex.getMessage:Any, ex:Any)
      }
    }
  }

  def clear(): Unit = {
    try {
      Files.delete(Paths.get(path))
    }catch{
      case ex:Throwable => logger.error("clear failed Exception = [{}]", ex.getMessage:Any, ex:Any)
    }
  }
}

object StoreRocksDB{
  def apply(path:String): StoreRocksDB = new StoreRocksDB(path)
}
