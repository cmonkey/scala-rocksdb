package org.excavator.boot.rocksdb.test

import java.nio.file.{Files, Paths}

import org.excavator.boot.rocksdb

import scala.collection.mutable.ListBuffer

object StoreRocksDBApp extends App{

  val path = Paths.get("/tmp/RocksDB")
  if(!Files.exists(path)){
    Files.createDirectory(path)
  }

  val rocksDB = rocksdb.StoreRocksDB(path.toUri.getPath)

  val listBuffer = ListBuffer[Tuple2[String, String]]()

  for(i <- 0 until 10000000){
    val key = "foo1_" + i
    val value = "fvv_" + i

    val tuple2 = Tuple2(key, value)

    listBuffer += tuple2
  }

  //val list = listBuffer.toList

  //rocksDB.putList(list)

  val searchStartTime = System.currentTimeMillis()
  println(s"start search time = ${searchStartTime}")
  val index = 10000000 - 993
  val key = "foo1_" + index
  rocksDB.get(key) match {
    case Some(value) => {
      val currentTime = System.currentTimeMillis()
      println(s"search end time = ${currentTime}")
      val searchTime = currentTime - searchStartTime
      println(s"get ${key} by value = ${value} and search time = ${searchTime}")
    }
    case None => println(s"search ${key} by value is null")
  }

}
