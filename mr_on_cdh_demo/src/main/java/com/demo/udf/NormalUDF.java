package com.demo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NormalUDF extends UDF {
    //手动添加evaluate方法并实现
    public Object evaluate(Object[] params){
        return new Object();
    }
}
