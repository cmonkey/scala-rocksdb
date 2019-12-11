package org.excavator.boot.rocksdb.test

import org.excavator.boot.rocksdb.StoreRocksDB
import org.junit.jupiter.api.{DisplayName, Test}
import org.junit.jupiter.api.Assertions._

import org.slf4j.LoggerFactory


class StoreRocksDBTests {
  val logger = LoggerFactory.getLogger(classOf[StoreRocksDBTests])

  val storeRocksDB: StoreRocksDB = StoreRocksDB()

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

}
