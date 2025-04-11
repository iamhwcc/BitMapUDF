package com.RoaringBitMap;

import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;

/**
 * @author stalwarthuang
 * @since 2025-04-09 星期三 23:08:40
 */
public class RoaringBitMapTest {
    public static void main(String[] args) {
        RoaringBitmap rbm1 = new RoaringBitmap();
        RoaringBitmap rbm2 = new RoaringBitmap();

        rbm1.add(1,6);
        rbm2.add(3, 8);
        System.out.println(rbm1);
        System.out.println();
        System.out.println(rbm2);
        System.out.println();
        rbm1.and(rbm2);
        System.out.println(rbm1);

    }
}
