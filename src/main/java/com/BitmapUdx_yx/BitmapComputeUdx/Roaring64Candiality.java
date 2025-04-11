package com.BitmapUdx_yx.BitmapComputeUdx;

import com.BitmapUdx_yx.RoaringBitmapBase;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class Roaring64Candiality extends GenericUDF {
    private transient BinaryObjectInspector OriInput;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length !=1){
            throw new UDFArgumentException("输入参数必须是一个");
        }
        OriInput =(BinaryObjectInspector)objectInspectors[0];
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
       if (null != deferredObjects[0] && deferredObjects[0].get() != null){
        byte[] input = OriInput.getPrimitiveJavaObject(deferredObjects[0].get());
           RoaringBitmapBase out = new RoaringBitmapBase();
           out.readByteArray(input);
           return out.getRoaring64Bitmap().getLongCardinality();
       }
       return 0L;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("Roaring64Candiality",strings,",");
    }
}
