package com.roaringbitmapudaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * 缓冲区
 *
 * @author stalwarthuang
 * @since 2025-04-11 星期五 16:41:13
 */
public class RoaringBitMapBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    private static final Logger log = LoggerFactory.getLogger(RoaringBitMapBuffer.class);

    private RoaringBitmap roaringBitMap;

    public RoaringBitMapBuffer() {
    }

    public RoaringBitMapBuffer(RoaringBitmap roaringBitmap) {
        this.roaringBitMap = roaringBitmap;
    }

    public RoaringBitmap getRoaringBitmap() {
        return this.roaringBitMap;
    }

    /**
     * 初始化
     */
    public void init() {
        this.roaringBitMap = new RoaringBitmap();
    }

    /**
     * add a value
     */
    public void add(int value) {
        this.roaringBitMap.add(value);
    }

    /**
     * and
     */
    public void and(RoaringBitmap other) {
        this.roaringBitMap.and(other);
    }

    /**
     * or
     */
    public void or(RoaringBitmap other) {
        this.roaringBitMap.or(other);
    }

    /**
     * xor
     */
    public void xor(RoaringBitmap other) {
        this.roaringBitMap.xor(other);
    }

//    /**
//     * 和缓冲区的BitMap进行and运算
//     *
//     * @param byteInput
//     */
//    public void mergeAnd(byte[] byteInput) {
//        if (byteInput == null) {
//            return;
//        }
//        // 初始化一个RoaringBitmap partial
//        RoaringBitmap partial = new RoaringBitmap();
//        // DataInputStream包装，后续用于反序列化
//        DataInputStream outputStream = new DataInputStream(new ByteArrayInputStream(byteInput));
//        try {
//            // 反序列化 「byte[] -> RoaringBitmap」
//            partial.deserialize(outputStream);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        // 如果this.roaringBitMap是null， 说明是第一次，就让他等于partial
//        if (this.roaringBitMap == null) {
//            this.roaringBitMap = partial;
//        } else {
//            this.roaringBitMap.and(partial);
//        }
//    }
//
//    public void mergeOr(byte[] byteInput) {
//        if (byteInput == null) {
//            return;
//        }
//        RoaringBitmap partial = new RoaringBitmap();
//        DataInputStream outputStream = new DataInputStream(new ByteArrayInputStream(byteInput));
//        try {
//            partial.deserialize(outputStream);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        if (this.roaringBitMap == null) {
//            this.roaringBitMap = partial;
//        } else {
//            this.roaringBitMap.or(partial);
//        }
//    }

//    public byte[] terminalPartial() {
//        if (null == roaringBitMap) {
//            return null;
//        }
//        try {
//            ByteArrayOutputStream byteout = new ByteArrayOutputStream();
//            DataOutputStream outputStream = new DataOutputStream(byteout);
//            roaringBitMap.serialize(outputStream);
//            outputStream.close();
//            return byteout.toByteArray();
//        } catch (IOException e) {
//            log.error(e.getMessage());
//            e.printStackTrace();
//        }
//        return null;
//    }
}
