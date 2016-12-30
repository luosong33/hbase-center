package cn.ls.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

/**
 * Created by Administrator on 2016/12/30.
 */
public class HbaseConn {

    static Configuration config = null;
    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node1,node2,node3");
        config.set("hbase.zookeeper.property.clientPort", "2181");
    }

    Connection conn = null;
    {
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection getHbaseConn(){
        return new HbaseConn().conn;
    }

    public static Configuration getHbaseConf(){
        return config;
    }

}
