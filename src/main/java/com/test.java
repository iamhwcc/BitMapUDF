package com;

import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;

/**
 * @author stalwarthuang
 * @description
 * @since 2025-04-14 星期一 12:04:17
 */
public class test {
    public static void main(String[] args) {
        RoaringBitmap btm = new RoaringBitmap();
        btm.add(1,7);
        for(int value:btm) {
            System.out.println(value);
        }
    }
}
