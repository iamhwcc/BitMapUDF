package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.List;

/**
 * @author stalwarthuang
 * @description 数组转BitMap，并以byte[]输出至hive存储
 * @since 2025-04-12 星期六 17:55:07
 */
@Description(name = "array_to_bitmap", value = "convert a array to a bitmap")
public class ArrayToBitMapUDF extends GenericUDF {
    // array类型检查器
    private ListObjectInspector listOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("ArrayToBitMapUDF requires exactly one argument");
        }
        ObjectInspector o = objectInspectors[0];
        if (!(o instanceof ListObjectInspector)) {
            throw new UDFArgumentException("ArrayToBitMapUDF requires a list argument");
        }
        this.listOI = (ListObjectInspector) o;
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List<?> list = this.listOI.getList(deferredObjects[0].get());
        RoaringBitmap bitmap = new RoaringBitmap();
        for (Object o : list) {
            bitmap.add(Integer.parseInt(o.toString()));
        }
        try {
            return BitMapUtil.serializeToBytes(bitmap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "array_to_bitmap(array)";
    }
}
