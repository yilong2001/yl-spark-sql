package com.spark.sql.examples;

import java.io.Serializable;

/**
 * Created by yilong on 2019/7/31.
 */
public class JUdfEx implements Serializable {
    String file;
    public JUdfEx(String file){
        this.file = file;
    }

    public int predict(Double[] args) {
        return 0;
    }
}
