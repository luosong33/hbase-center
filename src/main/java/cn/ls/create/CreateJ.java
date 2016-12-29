package cn.ls.create;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by Administrator on 2016/12/27.
 */
public class CreateJ {

    static Configuration config = null;
    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node1,node2,node3");
        config.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /*Connection conn = null;
    {
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public static void main(String[] args) throws IOException {

        HBaseAdmin admin = new HBaseAdmin(config);
//        Admin admin = null;
//        admin = new CreateJ().conn.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(args[0]);
        HColumnDescriptor family1 = new HColumnDescriptor("f1");
        desc.addFamily(family1);
        admin.createTable(desc);

    }

}
