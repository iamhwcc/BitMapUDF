package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

/**
 * @author stalwarthuang
 * @since 2025-04-12 星期六 17:46:28
 */
@Description(name = "bitmap_xor", value = "return bitmap1 xor bitmap2")
public class BitMapXorUDF extends GenericUDF {
    private BinaryObjectInspector input1OI;
    private BinaryObjectInspector input2OI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("bitmap_xor expects 2 arguments");
        }
        ObjectInspector arg1 = objectInspectors[0];
        ObjectInspector arg2 = objectInspectors[1];
        if (!(arg1 instanceof BinaryObjectInspector) || !(arg2 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("bitmap_xor expects binary arguments");
        }
        this.input1OI = (BinaryObjectInspector) arg1;
        this.input2OI = (BinaryObjectInspector) arg2;
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        byte[] byte1 = this.input1OI.getPrimitiveJavaObject(deferredObjects[0].get());
        byte[] byte2 = this.input2OI.getPrimitiveJavaObject(deferredObjects[1].get());
        try {
            RoaringBitmap btm1 = BitMapUtil.deserializeFromBytes(byte1);
            RoaringBitmap btm2 = BitMapUtil.deserializeFromBytes(byte2);
            btm1.xor(btm2);
            return BitMapUtil.serializeToBytes(btm1);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "bitmap_xor(bitmap1, bitmap2)";
    }
}
