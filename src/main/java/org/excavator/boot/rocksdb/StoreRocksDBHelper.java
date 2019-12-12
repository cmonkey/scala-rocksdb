package org.excavator.boot.rocksdb;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StoreRocksDBHelper {
    public static final Logger logger = LoggerFactory.getLogger(StoreRocksDBHelper.class);

    public static List<Tuple2<String, String>> getBefore(String keyPrefix, RocksDB rocksDB){

        var iterator = rocksDB.newIterator();

        var charset = StandardCharsets.UTF_8;

        var list = new ArrayList<Tuple2<String, String>>();

        for(iterator.seek(keyPrefix.getBytes(charset)); iterator.isValid(); iterator.next()){
            var key = new String(iterator.key(), charset);
            var value = new String(iterator.value(), charset);

            var tuple2 = Tuple2.apply(key, value);

            list.add(tuple2);
        }

        logger.info("getBefore list size = [{}]", list.size());
        return list;
    }
}
