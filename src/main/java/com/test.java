package com;

import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author stalwarthuang
 * @description
 * @since 2025-04-14 星期一 12:04:17
 */
public class test {
    public static void main(String[] args) {
        Integer[] nums1 = new Integer[] { 1, 2, 3, 4, 5 };
        Arrays.stream(nums1).forEach(System.out::println);
    }
}
