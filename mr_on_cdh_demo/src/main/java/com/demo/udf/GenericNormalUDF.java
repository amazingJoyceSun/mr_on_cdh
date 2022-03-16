package com.demo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericNormalUDF extends GenericUDF {
    //必须实现的三个方法
    //初始化参数
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //udf最先进入的方法 先判断调用时输入的参数个数是否符合函数要求
        if(objectInspectors.length!=2)
            throw new UDFArgumentException(" must have 2 arguments!");
        //计算时需要的参数类型
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String str1 = String.valueOf(deferredObjects[0].get());
        String str2 = String.valueOf(deferredObjects[1].get());
        //process Object
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert(strings.length ==2);
        return "arguments contains("+strings[0]+","+strings[1]+")";
    }
}
