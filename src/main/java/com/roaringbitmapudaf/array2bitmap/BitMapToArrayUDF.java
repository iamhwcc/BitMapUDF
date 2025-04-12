package com.roaringbitmapudaf.array2bitmap;

import com.roaringbitmapudaf.BitMapUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author stalwarthuang
 * @since 2025-04-12 星期六 16:51:51
 */
@Description(name = "bitmap_to_array", value = "bitmap to a array")
public class BitMapToArrayUDF extends GenericUDF {
    private BinaryObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1) {
            throw new UDFArgumentException("bitmap_to_array() takes exactly one argument");
        }
        ObjectInspector o = objectInspectors[0];
        if(!(o instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("argument is not a BinaryObjectInspector");
        }
        this.inputOI = (BinaryObjectInspector) o;
        // Array固定写法，里面是int
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        byte[] bytes = this.inputOI.getPrimitiveJavaObject(deferredObjects[0].get());
        List<Integer> res = new ArrayList<>();
        try {
            Iterator<Integer> iterator = BitMapUtil.deserializeFromBytes(bytes).iterator();
            while(iterator.hasNext()) {
                res.add(iterator.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "bitmap_to_array(bitmap)";
    }
}
