package org.excavator.boot.rocksdb.test

import java.nio.file.Files

import org.excavator.boot.rocksdb.StoreRocksDB
import org.junit.jupiter.api.{AfterAll, BeforeAll, DisplayName, Test}
import org.junit.jupiter.api.Assertions._
import org.slf4j.LoggerFactory


class StoreRocksDBTests {
  val logger = LoggerFactory.getLogger(classOf[StoreRocksDBTests])

  import StoreRocksDBTests._

  @Test
  @DisplayName("test rocks db is Open")
  def testRocksDBStatus(): Unit = {
    assertEquals(true, storeRocksDB.isOpen)
  }

  @Test
  @DisplayName("test rocks db write/read")
  def testRocksDBWriteReads(): Unit = {
    val key = "cmonkey"
    val value = "foo"

    storeRocksDB.put(key, value)

    storeRocksDB.get(key) match {
      case None => logger.error("get key failed")
      case Some(v) => assertEquals(v, value)
    }
  }

  @Test
  @DisplayName("test batch write")
  def testRocksDBBatchWrite(): Unit = {
    val key = "cmonkey"
    val value = "foo"

    val tuple2 = Tuple2(key, value)

    val list = List.fill(1000000)(tuple2)

    storeRocksDB.putList(list)
  }

}

object StoreRocksDBTests{
  var storeRocksDB:StoreRocksDB = null
  val path = Files.createTempDirectory("RocksDB")
  @BeforeAll
  @DisplayName("before all in init RocksDB")
  def init(): Unit = {
    storeRocksDB = StoreRocksDB(path.toUri.getPath)
  }

  @AfterAll
  @DisplayName("after all in shutdown store RocksDB")
  def clear(): Unit = {
    storeRocksDB.shutdown()
  }
}
