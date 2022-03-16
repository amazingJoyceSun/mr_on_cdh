package com.demo.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;

import java.io.IOException;

public class DemoReducer extends Reducer<Text,Text, WritableComparable, HCatRecord> {
    HCatSchema schema;
    HCatRecord record;

    @Override
    protected void setup(Reducer<Text, Text, WritableComparable, HCatRecord>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        schema = HCatOutputFormat.getTableSchema(context.getConfiguration());
        //初始化一条记录数，一行的字段数
        record = new DefaultHCatRecord(3);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, WritableComparable, HCatRecord>.Context context) throws IOException, InterruptedException {
        //do sth for filling record
    }

    @Override
    protected void cleanup(Reducer<Text, Text, WritableComparable, HCatRecord>.Context context) throws IOException, InterruptedException {
        context.write(null, record);
    }
}
