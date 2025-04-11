package com.BitmapUdx_yx.BitmapComputeUdx;

import com.BitmapUdx_yx.RoaringBitmapBase;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class Roaring64ContainsLong extends GenericUDF {
    private transient BinaryObjectInspector binInput;
    private transient LongObjectInspector LongInput;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length !=2){
            throw new UDFArgumentException("输入参数必须是两个");
        }
        if (PrimitiveObjectInspector.PrimitiveCategory.BINARY != ((PrimitiveObjectInspector)objectInspectors[0]).getPrimitiveCategory()
            || PrimitiveObjectInspector.PrimitiveCategory.LONG != ((PrimitiveObjectInspector)objectInspectors[1]).getPrimitiveCategory()
        ){
            throw new UDFArgumentException("输入参数必须是两个:第一个参数为BINARY第二个参数为Long类型");
        }
        binInput = (BinaryObjectInspector)objectInspectors[0];
        LongInput = (LongObjectInspector) objectInspectors[1];

        return  PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
       if (null == deferredObjects[0] || null == deferredObjects[1]){
           return  false;
       }
       byte[] bytesInput = this.binInput.getPrimitiveJavaObject(deferredObjects[0].get());
       Long value = PrimitiveObjectInspectorUtils.getLong(deferredObjects[1].get(),LongInput);
        RoaringBitmapBase bitmapBase = new RoaringBitmapBase();
        bitmapBase.readByteArray(bytesInput);
        if (bitmapBase.getRoaring64Bitmap().contains(value)){
            return true;
        }
        return  false;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("Roaring64ContainsLong",strings,",");
    }
}
