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
 * @description
 * @since 2025-04-14 星期一 15:16:20
 */
@Description(name = "bitmap_to_array", value = "convert a bitmap to string")
public class BitMapToString extends GenericUDF {
    private BinaryObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("bitmap_to_array() only takes exactly one arguments");
        }
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("bitmap_to_array() only take binary type arguments");
        }
        this.inputOI = (BinaryObjectInspector) arg;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        byte[] bytes = this.inputOI.getPrimitiveJavaObject(arguments[0].get());
        if (bytes == null) {
            return null;
        }
        try {
            RoaringBitmap btm = BitMapUtil.deserializeFromBytes(bytes);
            StringBuilder sb = new StringBuilder();
            for (int value : btm) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(value);
            }
            return sb.toString();
        } catch (IOException e) {
            throw new HiveException("Deserialization failed: " + e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "bitmap_to_array(bitmap)";
    }
}
