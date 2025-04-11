package com.BitmapUdx_yx;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.io.Writable;
import org.roaringbitmap.longlong.Roaring64Bitmap;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RoaringBitmapBaseWritable extends GenericUDAFEvaluator.AbstractAggregationBuffer implements Writable {

    private Roaring64Bitmap roaringBitmap;

    public RoaringBitmapBaseWritable(){
    }

    public RoaringBitmapBaseWritable(Roaring64Bitmap bitmap){
        this.roaringBitmap = bitmap;
    }

    public void setRoaringBitmap(Roaring64Bitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    public Roaring64Bitmap getRoaringBitmap() {
        return roaringBitmap;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (null == roaringBitmap){
            roaringBitmap = new Roaring64Bitmap();
            roaringBitmap.clear();
        }
        roaringBitmap.serialize(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        roaringBitmap = new Roaring64Bitmap();
        roaringBitmap.clear();
        roaringBitmap.deserialize(dataInput);
    }
}
