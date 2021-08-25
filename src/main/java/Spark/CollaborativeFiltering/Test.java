package Spark.CollaborativeFiltering;

import org.apache.spark.sql.sources.In;

import java.util.List;
import java.util.stream.Collectors;

public class Test {
    public static void main(String[] args) {
        String s = "GI251024341";
        int c1 = s.charAt(0);
        int c2 = s.charAt(1);
        System.out.println(s.substring(0,2));
        String key = "3251017342";
        System.out.println("parse int :: " + Long.parseLong(key));
        Long longKey = Long.parseLong(key);

        System.out.println("Long :: " +longKey);
        System.out.println("int :: " + (int)Long.parseLong(key));
//        System.out.println(Integer.parseInt(key));

//        String s = "GI251024341";
//        List<Integer> listOfIntegers = s.chars().boxed().collect(Collectors.toList());
//        String key = "";
//        for(int i:listOfIntegers) {
//            key += i;
//            System.out.println(i);
//        }
//        System.out.println(Integer.parseInt(key));

    }
}
