package com.roaringbitmapudaf.contains;

import com.roaringbitmapudaf.BitMapUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author stalwarthuang
 * @description
 * @since 2025-04-12 星期六 18:53:37
 */
@Description(name = "bitmap_contains_bitmap", value = "Checks if a bitmap contains all the elements of another bitmap.")
public class BitMapContainsBitMap extends GenericUDF {
    private BinaryObjectInspector input1OI;
    private BinaryObjectInspector input2OI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] parameters) throws UDFArgumentException {
        if (parameters.length != 2) {
            throw new UDFArgumentException("bitmap_contains_num expects 2 arguments");
        }
        ObjectInspector arg1 = parameters[0];
        ObjectInspector arg2 = parameters[1];
        if (!(arg1 instanceof BinaryObjectInspector) || !(arg2 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("Two parameters must be of type BinaryObjectInspector");
        }
        this.input1OI = (BinaryObjectInspector) arg1;
        this.input2OI = (BinaryObjectInspector) arg2;
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }
        byte[] bytes1 = this.input1OI.getPrimitiveJavaObject(deferredObjects[0].get());
        byte[] bytes2 = this.input2OI.getPrimitiveJavaObject(deferredObjects[1].get());
        try {
            RoaringBitmap btm1 = BitMapUtil.deserializeFromBytes(bytes1);
            RoaringBitmap btm2 = BitMapUtil.deserializeFromBytes(bytes2);
            if (btm2.getCardinality() > btm1.getCardinality()) {
                return false;
            }
            Iterator<Integer> iterator2 = btm2.iterator();
            while (iterator2.hasNext()) {
                if (!btm1.contains(iterator2.next())) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "bitmap_contains_bitmap(bitmap1, bitmap2)";
    }
}
