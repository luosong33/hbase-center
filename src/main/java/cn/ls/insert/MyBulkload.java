package cn.ls.insert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyBulkload {

    public static class MyBulkMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void setup(Mapper.Context context) throws IOException,
                InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 数据按\t切分组织, 也可以自定义的方式来解析, 比如复杂的json/xml文本行 
            String line = value.toString();
            String[] terms = line.split("\t");
            if (terms.length == 4) {
                byte[] rowkey = terms[0].getBytes();
                ImmutableBytesWritable imrowkey = new ImmutableBytesWritable(rowkey);
                // 写入context中, rowkey => keyvalue, 列族:列名  info:name, info:age, info:phone 
                context.write(imrowkey, new KeyValue(rowkey, Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(terms[1])));
                context.write(imrowkey, new KeyValue(rowkey, Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(terms[2])));
                context.write(imrowkey, new KeyValue(rowkey, Bytes.toBytes("info"), Bytes.toBytes("phone"), Bytes.toBytes(terms[3])));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: MyBulkload <table_name> <data_input_path> <hfile_output_path>");
            System.exit(2);
        }
        String tableName = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        // 创建的HTable实例用于, 用于获取导入表的元信息, 包括region的key范围划分 
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, tableName);

        Job job = Job.getInstance(conf, "MyBulkload");

        job.setMapperClass(MyBulkMapper.class);

        job.setJarByClass(MyBulkload.class);
        job.setInputFormatClass(TextInputFormat.class);

        // 最重要的配置代码, 需要重点分析 
        HFileOutputFormat.configureIncrementalLoad(job, table);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

} 