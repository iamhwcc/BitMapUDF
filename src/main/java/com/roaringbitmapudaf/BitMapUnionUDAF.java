package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

/**
 * @author stalwarthuang
 * @since 2025-04-12 星期六 17:00:32
 */
@Description(name = "bitmap_union", value = "聚合多个bitmap，并进行union去并集")
public class BitMapUnionUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        return new GenericEvaluate();
    }

    public static class GenericEvaluate extends GenericUDAFEvaluator {
        // 参数类型检查器，后续可调用检查器的方法获取参数
        private BinaryObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            this.inputOI = (BinaryObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            RoaringBitMapBuffer bf = new RoaringBitMapBuffer();
            reset(bf);
            return bf;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            bf.init();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            // 由于聚合组内的bitmap，所以直接merge
            Object o = objects[0];
            if (o != null) {
                merge(aggregationBuffer, o);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            byte[] bytes = this.inputOI.getPrimitiveJavaObject(o);
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                RoaringBitmap btm = BitMapUtil.deserializeFromBytes(bytes);
                bf.or(btm);
            } catch (IOException e) {
                throw new HiveException("Serialization failed: " + e.getMessage());
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                return BitMapUtil.serializeToBytes(bf.getRoaringBitmap());
            } catch (IOException e) {
                throw new HiveException("Serialization failed: " + e.getMessage());
            }
        }
    }
}
