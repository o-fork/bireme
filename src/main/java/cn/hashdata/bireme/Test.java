package cn.hashdata.bireme;

import com.alibaba.fastjson.JSONObject;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/28.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class Test {


    public static void main(String[] args) {


        TreeMap<Long,String> oneSuccess=new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long aLong, Long t1) {
                return t1.compareTo(aLong);
            }
        });



        System.out.println(oneSuccess.firstKey());


    }
}
