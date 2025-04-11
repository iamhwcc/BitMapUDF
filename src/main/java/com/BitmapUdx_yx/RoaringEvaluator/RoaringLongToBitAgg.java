package com.BitmapUdx_yx.RoaringEvaluator;

import com.BitmapUdx_yx.RoaringBitmapBase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class RoaringLongToBitAgg extends GenericUDAFEvaluator {
    private transient LongObjectInspector OriInput;
    private transient BinaryObjectInspector PartialInput;
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
         super.init(m, parameters);
         if (m == Mode.PARTIAL1 || m==Mode.COMPLETE){
             OriInput = (LongObjectInspector) parameters[0];
         }else {
             PartialInput = (BinaryObjectInspector) parameters[1];
         }
         return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        RoaringBitmapBase buff = new RoaringBitmapBase();
        reset(buff);
        return  buff;
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapBase rbf = (RoaringBitmapBase )aggregationBuffer;
        rbf.InitRoaring64();
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if (objects[0] == null){
                return;
            }
            Long oriLong = PrimitiveObjectInspectorUtils.getLong(objects[0],OriInput);
            RoaringBitmapBase rbw = (RoaringBitmapBase) aggregationBuffer;
            rbw.addLong(oriLong);

    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return terminate(aggregationBuffer);
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
        if (null == o){
            return;
        }
        RoaringBitmapBase rbw = (RoaringBitmapBase) aggregationBuffer;
        byte[] byinput = this.PartialInput.getPrimitiveJavaObject(o);
        rbw.mergeAggOr(byinput);
    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapBase terminal = (RoaringBitmapBase) aggregationBuffer;
        return terminal.terminalPartial();
    }
}
