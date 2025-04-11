package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * @author stalwarthuang
 * @since 2025-04-11 星期五 21:18:48
 */
@Description(name = "to_bitmap", value = "return a bimap")
public class To_BitMap extends AbstractGenericUDAFResolver {

    /**
     * 检查
     * @param parameters
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Expected exactly one argument");
        }
        return new GenericEvaluate();
    }

    public class GenericEvaluate extends GenericUDAFEvaluator {
        // PARTIAL1：原始数据到部分聚合，调用iterate和terminatePartial --> map阶段
        // PARTIAL2: 部分聚合到部分聚合，调用merge和terminatePartial --> combine阶段
        // FINAL: 部分聚合到完全聚合，调用merge和terminate --> reduce阶段
        // COMPLETE: 从原始数据直接到完全聚合 --> map阶段，并且没有reduce
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
        private BinaryObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                this.inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                this.internalMergeOI = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            // new一个新的Buffer
            RoaringBitMapBuffer bf = new RoaringBitMapBuffer();
            // 重置
            reset(bf);
            return bf;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            // 拿到缓冲区
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            // 调用重置方法
            bf.init();
        }

        /**
         * 每条数据和缓冲区进行运算
         * @param aggregationBuffer
         * @param objects
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            // 一条数据
            Object o = objects[0];
            if (o == null) {
                return;
            }
            // 拿到缓冲区
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                // 转换为int
                int row = PrimitiveObjectInspectorUtils.getInt(o, inputOI);
                // 加入缓冲区
                bf.add(row);
            } catch (NumberFormatException e) {
                throw new HiveException(e);
            }

        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            // 拿到缓冲区
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            // 别人的缓冲区
            byte[] bytes = this.internalMergeOI.getPrimitiveJavaObject(o);
            try {
                // 把别人的缓冲区byte[] 反序列化后，和自己or运算
                bf.or(BitMapUtil.deserializeFromBytes(bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            // 拿到缓冲区
            RoaringBitMapBuffer bf = (RoaringBitMapBuffer) aggregationBuffer;
            try {
                // 缓冲区序列化为byte[]返回
                return BitMapUtil.serializeToBytes(bf.getRoaringBitmap());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
