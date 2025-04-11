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
 * @since 2025-04-11 星期五 22:38:35
 */
@Description(name = "bitmap_or", value = "return bitmap1 or bitmap2")
public class BitMapOrUDF extends GenericUDF {
    private BinaryObjectInspector arg1OI;
    private BinaryObjectInspector arg2OI;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 2) {
            throw new UDFArgumentException("bitmap_or() takes exactly two arguments");
        }
        ObjectInspector arg1 = objectInspectors[0];
        ObjectInspector arg2 = objectInspectors[1];
        if(!(arg1 instanceof BinaryObjectInspector) || !(arg2 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("btm1 and btm2 must be binary objects");
        }
        this.arg1OI = (BinaryObjectInspector) arg1;
        this.arg2OI = (BinaryObjectInspector) arg2;
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        byte[] a = this.arg1OI.getPrimitiveJavaObject(deferredObjects[0].get());
        byte[] b = this.arg2OI.getPrimitiveJavaObject(deferredObjects[1].get());
        try {
            RoaringBitmap btm1 = BitMapUtil.deserializeFromBytes(a);
            RoaringBitmap btm2 = BitMapUtil.deserializeFromBytes(b);
            btm1.or(btm2);
            return BitMapUtil.serializeToBytes(btm1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Usage: bitmap_or(bitmap1, bitmap2)";
    }
}
