package com.roaringbitmapudaf;

import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

/**
 * BitMap 序列化 & 反序列化工具函数
 *
 * @author stalwarthuang
 * @since 2025-04-11 星期五 21:53:10
 */
@Slf4j
public class BitMapUtil {
    /**
     * 序列化 RoaringBitMap -> byte[]
     *
     * @param bitmap
     * @return
     * @throws IOException
     */
    public static byte[] serializeToBytes(RoaringBitmap bitmap) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(stream);
        bitmap.serialize(out);
        out.close();
        return stream.toByteArray();
    }

    /**
     * 反序列化 byte[] -> RoaringBitMap
     *
     * @param bytes
     * @return
     * @throws IOException
     */
    public static RoaringBitmap deserializeFromBytes(byte[] bytes) throws IOException {
        RoaringBitmap btm = new RoaringBitmap();
        if (bytes == null) {
            return btm;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(stream);
        btm.deserialize(in);
        in.close();
        return btm;
    }
}
