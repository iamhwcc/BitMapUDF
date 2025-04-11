package com.BitmapUdx_yx.RoaringEvaluator;

import com.BitmapUdx_yx.RoaringBitmapBase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class RoaringBitOrBitAgg extends GenericUDAFEvaluator {
    private  transient BinaryObjectInspector OriInput;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);
        OriInput =(BinaryObjectInspector) parameters[0];
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        RoaringBitmapBase buff = new RoaringBitmapBase();
        reset(buff);
        return buff;
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapBase rbw = (RoaringBitmapBase) aggregationBuffer;
        rbw.InitRoaring64();
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        if (objects[0] == null){
            return;
        }
        byte[] byteinput = this.OriInput.getPrimitiveJavaObject(objects[0]);
        RoaringBitmapBase rbw = (RoaringBitmapBase) aggregationBuffer;
        rbw.mergeAggOr(byteinput);
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return terminate(aggregationBuffer);
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
        if (o == null){
            return;
        }
        RoaringBitmapBase buff1 = (RoaringBitmapBase) aggregationBuffer;
        byte[] bytesPartial = this.OriInput.getPrimitiveJavaObject(o);
        buff1.mergeAggOr(bytesPartial);
    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapBase termianl = (RoaringBitmapBase) aggregationBuffer;
        return termianl.terminalPartial();
    }
}
