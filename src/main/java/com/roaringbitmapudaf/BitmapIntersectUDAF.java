package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
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
 * @description
 * @since 2025-04-12 星期六 18:11:10
 */
@Description(name = "bitmap_intersect", value = "return intersection of some bitmaps")
public class BitmapIntersectUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] args) throws SemanticException {
        if (args.length != 1) {
            throw new UDFArgumentException(String.format("Exactly one argument is expected, but get %d", args.length));
        }
        return new IntersectEvaluator();
    }

    public static class IntersectEvaluator extends GenericUDAFEvaluator {
        private BinaryObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentException("Exactly one argument is expected");
            }
            ObjectInspector o = parameters[0];
            if (!(o instanceof BinaryObjectInspector)) {
                throw new UDFArgumentException("Only BinaryObjectInspectors are accepted");
            }
            this.inputOI = (BinaryObjectInspector) o;
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
            if (o == null) return;
            byte[] bytes = this.inputOI.getPrimitiveJavaObject(o);
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                RoaringBitmap btm = BitMapUtil.deserializeFromBytes(bytes);
                if (bf.getRoaringBitmap().isEmpty()) {
                    bf.or(btm);
                } else {
                    bf.and(btm);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                return BitMapUtil.serializeToBytes(bf.getRoaringBitmap());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
