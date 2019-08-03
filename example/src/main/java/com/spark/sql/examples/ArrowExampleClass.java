package com.spark.sql.examples;

import java.util.Random;

/**
 * Created by yilong on 2019/7/23.
 */
public class ArrowExampleClass {
    public int label;
    public long aLong;
    //public byte[] arr;
    public float aFloat;
    public Random random;

    public ArrowExampleClass(Random random, int index){
        this.random = random;
        this.label = this.random.nextInt(100) % 2;
        this.aLong = this.random.nextInt(Integer.MAX_VALUE);
        //this.arr = new byte[this.random.nextInt(64)];
        //this.random.nextBytes(this.arr);
        this.aFloat = this.random.nextFloat();
    }

    public static String firstX(byte[] data, int items){
        int toProcess = Math.min(items, data.length);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < toProcess; i++) {
            sb.append(String.format("0x%02x", data[i])+ " ");
        }
        return sb.toString();
    }

    public static long hashArray(byte[] data){
        long ret = 0;
        for(int i = 0; i < data.length;i++)
            ret+=data[i];
        return ret;
    }

    public String toString() {
        return  label + "\t | " +
                + aLong + "\t | " +
                //" arr[" + this.arr.length + "] " + firstX(this.arr, 5) + "\t | " +
                + aFloat;
    }

    public long getSumHash(){
        long ret = 0;
        ret+=label;
        ret+=aLong;
        //ret+=ArrowExampleClass.hashArray(this.arr);
        ret+=aFloat;
        return ret;
    }
}
