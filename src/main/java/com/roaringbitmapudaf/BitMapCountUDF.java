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
 * @since 2025-04-11 星期五 22:45:08
 */
@Description(name = "bitmap_count", value = "return cardinality of a bitmap")
public class BitMapCountUDF extends GenericUDF {
    private BinaryObjectInspector inputOI1;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("bitmap_count requires exactly one argument");
        }
        ObjectInspector objectInspector = objectInspectors[0];
        if (!(objectInspector instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("bitmap_count requires a binary object");
        }
        this.inputOI1 = (BinaryObjectInspector) objectInspectors[0];
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        byte[] bytes = this.inputOI1.getPrimitiveJavaObject(deferredObjects[0].get());
        RoaringBitmap btm = null;
        try {
            btm = BitMapUtil.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return btm.getCardinality();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "bitmap_count(bitmap)";
    }
}
