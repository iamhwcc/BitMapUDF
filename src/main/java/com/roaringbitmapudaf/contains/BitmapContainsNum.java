package com.roaringbitmapudaf.contains;

import com.roaringbitmapudaf.BitMapUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author stalwarthuang
 * @description
 * @since 2025-04-12 星期六 18:36:12
 */
@Description(name = "bitmap_contains_num", value = "Check if a bitmap contains the number n, return a boolean")
public class BitmapContainsNum extends GenericUDF {

    private BinaryObjectInspector inputOI1;
    private PrimitiveObjectInspector inputOI2;

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {
        if (parameters.length != 2) {
            throw new UDFArgumentException("bitmap_contains_num requires 2 arguments");
        }
        ObjectInspector arg1 = parameters[0];
        ObjectInspector arg2 = parameters[1];
        if (!(arg1 instanceof BinaryObjectInspector) || !(arg2 instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentException("The first parameter of bitmap_contains_num must be a byte[] " +
                    "and the second parameter of a int");
        }
        this.inputOI1 = (BinaryObjectInspector) arg1;
        this.inputOI2 = (PrimitiveObjectInspector) arg2;
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        // 拿到byte[]方法
        byte[] bytes = this.inputOI1.getPrimitiveJavaObject(deferredObjects[0].get());
        // 拿到int方法
        int n = PrimitiveObjectInspectorUtils.getInt(deferredObjects[1], this.inputOI2);
        try {
            RoaringBitmap btm = BitMapUtil.deserializeFromBytes(bytes);
            Iterator<Integer> iterator = btm.iterator();
            if (btm.isEmpty()) {
                return false;
            } else {
                while (iterator.hasNext()) {
                    if (!btm.contains(iterator.next())) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "bitmap_contains_num(bitmap, number)";
    }
}
