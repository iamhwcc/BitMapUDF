package com.BitmapUdx_yx.BitmapComputeUdx;

import com.BitmapUdx_yx.RoaringEvaluator.RoaringBitOrBitAgg;
import com.BitmapUdx_yx.RoaringEvaluator.RoaringLongToBitAgg;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class BitmapUDXAll extends AbstractGenericUDAFResolver {
    @SneakyThrows
    public GenericUDAFEvaluator genericUDAFEvaluator(TypeInfo[] info){
        if (info.length != 1){
            throw new UDFArgumentException("参数必须是1个参数输入!");
        }
        if (PrimitiveObjectInspector.PrimitiveCategory.LONG == ((PrimitiveTypeInfo)info[0]).getPrimitiveCategory()){
            return new RoaringLongToBitAgg();
        }
        if (PrimitiveObjectInspector.PrimitiveCategory.BINARY == ((PrimitiveTypeInfo)info[0]).getPrimitiveCategory()){
            return new RoaringBitOrBitAgg();
        }else {
            throw  new UDFArgumentException("处理的参数必须为LONG类型或者BINARY类型,Long类型的输入,则是对所有的Long数据进行聚合,返回的是BINARY序列之后的结果,BINARY则是对输入为二进制文件进行聚合,返回的是对应的聚合之后的BINARY类型,如果想得到具体的数值结果,需要调用对应的Candiality函数！");
        }
    }
}
