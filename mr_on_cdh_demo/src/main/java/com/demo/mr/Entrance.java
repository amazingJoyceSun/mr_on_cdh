package com.demo.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.util.*;

public class Entrance extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //conf.set("","");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Entrance.class);
        job.setMapperClass(DemoMapper.class);
        job.setReducerClass(DemoReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(WritableComparable.class);
        //设置输出类型为HCatRecord--代表hive表
        job.setOutputValueClass(HCatRecord.class);


        //初始化 HCatInputFormat，并指定数据库、表名、hive表分区
        HCatInputFormat.setInput(job, "input_dbname","input_tbname","ds = 20220101");
        //构造输出表的partition
        Map<String,String> partition = new HashMap<>();
        partition.put("ds","date");
        //初始化HCatOutputFormat
        HCatOutputFormat outputFormat = new HCatOutputFormat();
        OutputJobInfo outputJobInfo = OutputJobInfo.create("output_dbname","output_tbname",partition);
        outputFormat.setOutput(job, outputJobInfo);
        //构造输出表的schema-要和hive表的建表语句保持一致，保证hive中已经存在此表
        //构造schema有其他简便方式可自行摸索。
        List<HCatFieldSchema> fieldSchemas = new ArrayList<>();
        //word string 'comment-word'
        PrimitiveTypeInfo type = new PrimitiveTypeInfo();
        type.setTypeName("string");
        HCatFieldSchema fieldSchema = new HCatFieldSchema("word",type,"comment-word");
        fieldSchemas.add(fieldSchema);
        //cnt int 'comment-cnt'
        type = new PrimitiveTypeInfo();
        type.setTypeName("int");
        fieldSchema = new HCatFieldSchema("cnt",type,"comment-cnt");
        fieldSchemas.add(fieldSchema);
        //ds string 'comment-partition ds'
        type = new PrimitiveTypeInfo();
        type.setTypeName("string");
        fieldSchema = new HCatFieldSchema("ds",type,"comment-partition ds");
        fieldSchemas.add(fieldSchema);
        //指定schema
        HCatSchema out_schema = new HCatSchema(fieldSchemas);
        //HCatSchema schema = outputFormat.getTableSchema(job.getConfiguration());
        outputFormat.setSchema(job,out_schema);

        //指定HCatOutputFormat为outputformat
        job.setInputFormatClass(HCatInputFormat.class);
        //指定HCatInputFormat为inputformat
        job.setOutputFormatClass(HCatOutputFormat.class);
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new Entrance(), args);
        System.exit(exitCode);
    }
}
