package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author stalwarthuang
 * @since 2025-04-11 星期五 16:56:52
 */
public class BitMapAdd extends GenericUDAFEvaluator {
    private BinaryObjectInspector objectInspector;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);
        objectInspector = (BinaryObjectInspector) parameters[0];
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    /**
     * 获取新的一个Buffer
     * @return
     * @throws HiveException
     */
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        RoaringBitMapBuffer bf = new RoaringBitMapBuffer();
        reset(bf);
        return bf;
    }

    /**
     * 重置buffer
     * @param aggregationBuffer
     * @throws HiveException
     */
    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
        bf.init();
    }

    /**
     * 遍历每条数据，和缓冲区进行运算(and & or & xor)
     * @param aggregationBuffer
     * @param objects
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        if(objects[0] == null) {
            return;
        }
        byte[] byteInput = this.objectInspector.getPrimitiveJavaObject(objects[0]);
        RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
        bf.mergeAnd(byteInput);
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return terminate(aggregationBuffer);
    }

    /**
     * 合并
     * @param aggregationBuffer
     * @param o
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
        if(o == null) {
            return;
        }
        byte[] byteInput = this.objectInspector.getPrimitiveJavaObject(o);
        RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
        bf.mergeAnd(byteInput);
    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
        return bf.terminalPartial();
    }
}
