package com.demo.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class GenericNormalEvaluator extends GenericUDAFEvaluator implements Serializable {

    private transient ObjectInspector input_OI;
    //存对象
    private transient ObjectInspector combine_OI;
    //存list（如果有list需求可以使用，没有可不用）
    private transient StandardListObjectInspector internalMergeOI;
    //在map写到reduce过程中，缓冲区buffer指定的数据类型
    enum BufferType { Object }
    private BufferType bufferType;

    public BufferType getBufferType() {
        return bufferType;
    }
    public void setBufferType(BufferType bufferType) {
        this.bufferType = bufferType;
    }
    public  GenericNormalEvaluator(BufferType bufferType) {
        this.bufferType = bufferType;
    }
    class GenericAggregationBuffer extends AbstractAggregationBuffer {
        //指定缓冲区容器类型
        private Object container;
        public GenericAggregationBuffer() {
            if (bufferType == BufferType.Object){
                container = new Object();
            } else
                throw new RuntimeException("Buffer type unknown");
        }
    }
    // 确定各个阶段输入输出参数的数据格式ObjectInspectors
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        //map阶段
        if (m == Mode.PARTIAL1) {
            assert(parameters.length>=2);
            input_OI = parameters[0];///parameters[1];
            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector
            );

        }
        else {
            if(!(parameters[0] instanceof StandardListObjectInspector)){
                combine_OI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                return combine_OI;
            }else {
                internalMergeOI = (StandardListObjectInspector) parameters[0];
                combine_OI = internalMergeOI.getListElementObjectInspector();
                return combine_OI;
            }
        }
    }
    // 重置聚集结果
    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        //重置操作，清空缓冲区的容器
        ((GenericAggregationBuffer)aggregationBuffer).container = null;
    }
    // 新建保存数据聚集结果的buffer对象
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        GenericAggregationBuffer buffer = new GenericAggregationBuffer();
        return buffer;
    }

    // map阶段，迭代处理输入sql传过来的列数据
    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        //process container
        Object params = objects[0];
        //给 input_OI 赋值，persist保证取内存中 input_OI 最新的值
        Object obj = ObjectInspectorUtils.copyToStandardObject(params,input_OI);
        //对缓冲区容器数据操作（此处直接赋值，也可以做其他操作。list则为add，int则可为累加等操作）
        ((GenericAggregationBuffer)aggregationBuffer).container = obj;
    }
    // combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
    //聚合所有处理后的分片结果
    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

        GenericAggregationBuffer buffer = (GenericAggregationBuffer) aggregationBuffer;
        Object obj = (Object)internalMergeOI.getListElement(new Object(),1);
        //对缓冲区容器数据操作（此处直接赋值，也可以做其他操作。list则为add，int则可为累加等操作）
        buffer.container = obj;
    }
    // map与combiner结束返回结果，得到部分数据聚集结果
    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        GenericAggregationBuffer buffer = (GenericAggregationBuffer) aggregationBuffer;
        //获取缓冲区容器中的所有内容
        return  buffer.container;
    }
    // reduce阶段，输出最终结果
    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        GenericAggregationBuffer buffer = (GenericAggregationBuffer) aggregationBuffer;
        //获取缓冲区容器中的所有内容
        Object obj = buffer.container;
        //process obj
        return obj;
    }







}
