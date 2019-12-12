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

  val keyPrefix = "key_"
  val valuePrefix = "value_"

  val listBuffer = ListBuffer[Tuple2[String, String]]()

  for(i <- 0 until 10){
    val key = keyPrefix + i
    val value = valuePrefix + i

    val tuple2 = Tuple2(key, value)

    listBuffer += tuple2
  }

  val list = listBuffer.toList

  rocksDB.putList(list)

  val searchStartTime = System.currentTimeMillis()
  println(s"start search time = ${searchStartTime}")
  val index = 1000 - 7

  val getKey = keyPrefix + index

  rocksDB.get(getKey) match {
    case Some(value) => {
      val currentTime = System.currentTimeMillis()
      println(s"search end time = ${currentTime}")
      val searchTime = currentTime - searchStartTime
      println(s"get ${getKey} by value = ${value} and search time = ${searchTime}")
    }
    case None => println(s"search ${getKey} by value is null")
  }

  rocksDB.getBefore(keyPrefix).foreach(elem => {
    println(s"iterator key = ${elem._1} value = ${elem._2}")
  })

}
