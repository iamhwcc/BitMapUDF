package com.BitmapUdx_yx;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;

@Slf4j
/**
 * UDAF 缓冲区
 */
public class RoaringBitmapBase extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    private Roaring64NavigableMap roaring64Bitmap;

    public RoaringBitmapBase() {
    }

    public RoaringBitmapBase(Roaring64NavigableMap roaring64Bitmap) {
        this.roaring64Bitmap = roaring64Bitmap;
    }

    public void setRoaring64Bitmap(Roaring64NavigableMap roaring64Bitmap) {
        this.roaring64Bitmap = roaring64Bitmap;
    }

    public Roaring64NavigableMap getRoaring64Bitmap() {
        return roaring64Bitmap;
    }

    public void InitRoaring64() {
        this.roaring64Bitmap = new Roaring64NavigableMap();
        this.roaring64Bitmap.clear();
    }

    public void addLong(long value) {
        this.roaring64Bitmap.addLong(value);
    }

    public byte[] terminalPartial() {
        if (null == roaring64Bitmap) {
            return null;
        }
        try {
            ByteArrayOutputStream byteout = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteout);
            roaring64Bitmap.serialize(outputStream);
            outputStream.close();
            return byteout.toByteArray();
        } catch (IOException e) {
            log.error("反序列化失败");
            e.printStackTrace();
        }
        return null;
    }

    public void readByteArray(byte[] byteInput) {
        roaring64Bitmap = new Roaring64NavigableMap();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(byteInput))) {
            roaring64Bitmap.clear();
            roaring64Bitmap.deserialize(input);
        } catch (IOException e) {
            log.error("序列化失败");
            e.printStackTrace();
        }
    }

    public void mergeAggOr(byte[] byteInput) {
        if (byteInput == null) {
            return;
        }
        Roaring64NavigableMap partial = new Roaring64NavigableMap();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteInput));
        try {
            partial.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (this.roaring64Bitmap == null) {
            this.roaring64Bitmap = partial;
        } else {
            this.roaring64Bitmap.or(partial);
        }
    }

    public void mergeAggAnd(byte[] byteInput) {
        if (byteInput == null) {
            return;
        }
        Roaring64NavigableMap partial = new Roaring64NavigableMap();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteInput));
        try {
            partial.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (this.roaring64Bitmap == null) {
            this.roaring64Bitmap = partial;
        } else {
            this.roaring64Bitmap.and(partial);
        }
    }

    public void mergeAggAndNot(byte[] byteInput) {
        if (byteInput == null) {
            return;
        }
        Roaring64NavigableMap partial = new Roaring64NavigableMap();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteInput));
        try {
            partial.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (this.roaring64Bitmap == null) {
            this.roaring64Bitmap = partial;
        } else {
            this.roaring64Bitmap.andNot(partial);
        }
    }
}
