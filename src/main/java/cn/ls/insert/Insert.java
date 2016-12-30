package cn.ls.insert;

import cn.ls.util.HbaseConn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/12/30.
 */
public class Insert {

    public static void main(String[] args) throws IOException {

        /*Connection hbaseConn = new HbaseConn().getHbaseConn();
        HTableInterface table = (HTableInterface) hbaseConn.getTable(TableName.valueOf(args[0]));
        Put put = new Put(Bytes.toBytes(args[1])); // zookey
        put.add(Bytes.toBytes(args[2]), Bytes.toBytes(args[3]),Bytes.toBytes(args[4])); // 列族，qualifier，value
        table.put(put);
        table.close();// 释放资源
        hbaseConn.close();*/

        /*hbase里面其实没有列的概念，列就是数据*/
//        insertOne();
        insertBatch();

    }

    private static void insertBatch() throws IOException {
        // 批量插入
        Connection hbaseConn = new HbaseConn().getHbaseConn();
        HTableInterface table = (HTableInterface) hbaseConn.getTable(TableName.valueOf("testCreate1228"));
        List<Put> list = new ArrayList<Put>();
        for (int i =0; i< 10; i++){
            Put put = new Put(Bytes.toBytes("20161130abc" + i));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("lie1"+i), Bytes.toBytes("20161130abc"+i));
            list.add(put);
        }
        table.put(list);
        table.close();
        hbaseConn.close();
    }

    private static void insertOne() throws IOException {
        // 插入单条
        Connection hbaseConn = new HbaseConn().getHbaseConn();
        HTableInterface table = (HTableInterface) hbaseConn.getTable(TableName.valueOf("testCreate1228"));
        Put put = new Put(Bytes.toBytes("20161130abc3"));
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("lie1"), Bytes.toBytes("20161130abc"));
        table.put(put);
        table.close();
        hbaseConn.close();
    }

}
