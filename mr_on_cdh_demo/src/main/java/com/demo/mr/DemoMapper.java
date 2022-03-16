package com.demo.mr;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hcatalog.data.HCatRecord;
//import org.apache.hcatalog.data.schema.HCatSchema;
//import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DemoMapper extends Mapper<Object, HCatRecord, Text,Text>{
    HCatSchema schema;
    Text txt;
    @Override
    protected void setup(Mapper<Object, HCatRecord, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        txt = new Text();
        //获取输入hive表的schema信息
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
    }

    @Override
    protected void map(Object key, HCatRecord value, Mapper<Object, HCatRecord, Text, Text>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

}