package com.BitmapUdx_yx.BitmapComputeUdx;

import com.BitmapUdx_yx.RoaringBitmapBase;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Roaring64BinaryToLongUDTF extends GenericUDTF {
    private  transient BinaryObjectInspector inputBinary;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1){
            throw  new UDFArgumentException("参数必须是一个");
        }
        if (((PrimitiveObjectInspector)argOIs[0]).getPrimitiveCategory()!= PrimitiveObjectInspector.PrimitiveCategory.BINARY){
            throw  new UDFArgumentException("参数类型必须是binary");
        }
        inputBinary = (BinaryObjectInspector) argOIs[0];
        List<String> fieldname = new ArrayList<>();
        List<ObjectInspector> fields = new ArrayList<>();
        fieldname.add("id");
        fields.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldname,fields);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (null !=objects[0]){
            byte[] input = this.inputBinary.getPrimitiveJavaObject(objects[0]);
            RoaringBitmapBase out = new RoaringBitmapBase();
            out.readByteArray(input);
            Iterator<Long> iterator = out.getRoaring64Bitmap().iterator();
            while (iterator.hasNext()){
                forward(new Object[]{iterator.next()});
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
