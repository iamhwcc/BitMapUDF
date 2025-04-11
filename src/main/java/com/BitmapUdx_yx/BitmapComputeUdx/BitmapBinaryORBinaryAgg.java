package com.BitmapUdx_yx.BitmapComputeUdx;

import com.BitmapUdx_yx.RoaringEvaluator.RoaringBitOrBitAgg;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
public class BitmapBinaryORBinaryAgg extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1){
            throw new UDFArgumentException("参数必须是1个参数输入!");
        }
        return  new RoaringBitOrBitAgg();
    }
}
