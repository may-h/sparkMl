package Spark.CollaborativeFiltering;

import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class CollaborativeFiltering {

    public static String replace(String mainString, String oldString,
                                 String newString) {
        if (mainString == null) {
            return null;
        }
        if (oldString == null || oldString.length() == 0) {
            return mainString;
        }
        if (newString == null) {
            newString = "";
        }
        int i = mainString.lastIndexOf(oldString);
        if (i < 0)
            return mainString;
        StringBuffer mainSb = new StringBuffer(mainString);
        while (i >= 0) {
            mainSb.replace(i, (i + oldString.length()), newString);
            i = mainString.lastIndexOf(oldString, i - 1);
        }
        return mainSb.toString();
    }

    public static void main(String[] args) {
        System.out.println("[" + new Date() + "] process start");

        String baseMode = "";
        String output = "";
        String finaloutput = "";
        String thresholdStr = "";
        String targetIdStr = "";
        String masterLocal = "";
        String numStr = "";
        String rankStr = "";
        String numIterationsStr = "";
        String lambdaStr = "";
        String logLevel = "";
        String maxResultStr = "";
        boolean isDebug = false;

        Path currentPath = Paths.get(System.getProperty("user.dir"));
        String outputPath = (Paths.get(currentPath.toString(), "output", "output.json")).toString(); //default output path
        if (System.getProperty("outputPath") != null) {
            outputPath = System.getProperty("outputPath");
        }

        String dataFilePath = (Paths.get(currentPath.toString(), "data", "spark.txt")).toString(); //default input data path
        try {
            dataFilePath = args[1];
        } catch (Exception e) {
            if (System.getProperty("inputPath") != null) {
                dataFilePath = System.getProperty("inputPath");
            }
        }

        System.out.println("[" + new Date() + "] inputPath :: " + dataFilePath);
        System.out.println("[" + new Date() + "] outputPath :: " + outputPath);

        if (isDebug) System.out.println("dataFilePath:" + dataFilePath);
        if (isDebug) System.out.println("outputPath:" + outputPath);


        try {
            baseMode = args[0]; // item: item based, user: user based
            if (!baseMode.equals("item") && !baseMode.equals("user")) {
                throw new Error(baseMode + " mode is not valid. user/item is required");
            }
        } catch (Exception e) {
            baseMode = "user"; // user,item
        }
        System.out.println("[" + new Date() + "] mode type :: " + baseMode);

        try {
            String debug = System.getProperty("debug");
            if (debug == null) throw new Exception();
            if ("true".equals(debug)) isDebug = true;
        } catch (Exception e) {
            isDebug = false;
        }

        try {
            thresholdStr = System.getProperty("threshold");
            if (thresholdStr == null) {
                throw new Exception();
            }
        } catch (Exception e) {
            thresholdStr = "0.9";
        }
        if (isDebug) System.out.println("thresholdStr:" + thresholdStr);
        lambdaStr = thresholdStr;

        try {
//			targetIdStr = args[2];
            targetIdStr = System.getProperty("targetId");
            if (targetIdStr == null) {
                throw new Exception();
            }
        } catch (Exception e) {
            targetIdStr = "all";
        }
        if (isDebug) System.out.println("targetIdStr:" + targetIdStr);


        try {
            numStr = System.getProperty("num");
            if (numStr == null) throw new Exception();
        } catch (Exception e) {
            numStr = "100";
        }

        if (isDebug) System.out.println("numStr:" + numStr);

        try {
            rankStr = System.getProperty("rank");
            if (rankStr == null) throw new Exception();
        } catch (Exception e) {
            rankStr = "10";
        }
        if (isDebug) System.out.println("rankStr:" + rankStr);

        try {
            numIterationsStr = System.getProperty("numIterations");
            if (numIterationsStr == null) throw new Exception();
        } catch (Exception e) {
            numIterationsStr = "10";
        }

        if (isDebug) System.out.println("numIterationsStr:" + numIterationsStr);
        try {
            maxResultStr = System.getProperty("maxResultStr");
            if (maxResultStr == null) throw new Exception();
        } catch (Exception e) {
            maxResultStr = "30";
        }
        if (isDebug) System.out.println("maxResultStr:" + maxResultStr);

        if (System.getProperty("hadoop.home.dir") == null) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\Admin\\Desktop\\workspace\\hadoop-winutils-2.6.0");
        }


        try {
            masterLocal = System.getProperty("masterLocal");
            if (masterLocal == null) throw new Exception();
        } catch (Exception e) {
            masterLocal = "local[4]";
        }
        if (isDebug) System.out.println("masterLocal:" + masterLocal);
        try {
            logLevel = System.getProperty("logLevel");
            if (logLevel == null) throw new Exception();
        } catch (Exception e) {
            logLevel = "ERROR";
        }

        if ("ERROR".equals(logLevel)) {
            Logger.getLogger("apache").setLevel(Level.ERROR);
            Logger.getLogger("com").setLevel(Level.ERROR);
            Logger.getLogger("org").setLevel(Level.ERROR);
        } else if ("INFO".equals(logLevel)) {
            Logger.getLogger("apache").setLevel(Level.INFO);
            Logger.getLogger("com").setLevel(Level.INFO);
            Logger.getLogger("org").setLevel(Level.INFO);
        } else if ("WARN".equals(logLevel)) {
            Logger.getLogger("apache").setLevel(Level.WARN);
            Logger.getLogger("com").setLevel(Level.WARN);
            Logger.getLogger("org").setLevel(Level.WARN);
        }


        try {
            if ("all".equals(targetIdStr)) targetIdStr = "0";

            SparkConf conf = new SparkConf().setAppName("CF-Spark MLlib");
            if (!"".equals(masterLocal)) conf.setMaster(masterLocal);
            JavaSparkContext jsc = new JavaSparkContext(conf);

            //--------------------------------------------
            int targetId = Integer.parseInt(targetIdStr);
            int num = Integer.parseInt(numStr);
            int rank = Integer.parseInt(rankStr);
            int numIterations = Integer.parseInt(numIterationsStr);
            double lambda = Double.parseDouble(lambdaStr);
            //--------------------------------------------

            // Load and parse the data
//			String path = dataFilePath;

            // 임의로 데이터 파일 경로 넣어줌
            System.out.println("dataFilePath : " + dataFilePath);
//			dataFilePath = "C:\\Users\\Admin\\Desktop\\workspace\\output_backup\\spark_input\\test.txt";
//			dataFilePath = "/home/sop/test/data/spark.tmp";
//			dataFilePath = "C:\\Users\\Admin\\AppData\\Local\\Temp\\tempFile_8715259122777345222.tmp";
            JavaRDD<String> data = jsc.textFile(dataFilePath);


            Map<String, String> map = new HashMap<>();
            BufferedReader reader = new BufferedReader(
                    new FileReader(dataFilePath)
            );
            String str;
            System.out.println("[" + new Date() + "] map start");
            if (baseMode.equals("item")) {
                while ((str = reader.readLine()) != null) {
                    String key = str.split(",")[3];
                    String value = str.split(",")[0];
//				System.out.println("item mode - " + key + " :: " + value);
                    map.put(key, value);
                }
            } else { //user
                while ((str = reader.readLine()) != null) {
                    String key = str.split(",")[3];
                    String value = str.split(",")[1];
//				System.out.println("user mode - " + key + " :: " + value);
                    map.put(key, value);
                }
            }
            System.out.println("[" + new Date() + "] map finished");
            reader.close();


            // Mapping Table Create
            System.out.println("[" + new Date() + "] ratings start");
            String finalBaseMode = baseMode;
            JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
                private static final long serialVersionUID = 571408671101770835L;

                public Rating call(String s) {
                    String[] sarray = s.split("[,]");
                    if (finalBaseMode.equals("item")) { //item
                        return new Rating(Integer.parseInt(sarray[3]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    } else { //user
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[3]),
                                Double.parseDouble(sarray[2]));
                    }
                }

                ;
            }).cache();
            System.out.println("[" + new Date() + "] ratings finished");


            // Build the recommendation model using ALS
            System.out.println("[" + new Date() + "] train start");
            MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, lambda);
            System.out.println("[" + new Date() + "] map size : " + map.size());

            System.out.println("[" + new Date() + "] ratings2 start");
            JavaRDD<Rating> ratings2 = ratings.filter(new Function<Rating, Boolean>() {
                private static final long serialVersionUID = 8900885706131295795L;

                public Boolean call(Rating rating) {
                    boolean ret = false;
                    if (rating.user() == targetId) ret = true;
                    if (targetId == 0) ret = true;
                    return ret;
                }
            }).cache();
            System.out.println("[" + new Date() + "] ratings2 finished");

            System.out.println("[" + new Date() + "] userRating start");
            JavaRDD<Tuple2<Object, Rating[]>> userRating = model.recommendProductsForUsers(num).toJavaRDD().filter(new Function<Tuple2<Object, Rating[]>, Boolean>() {
                private static final long serialVersionUID = 2333498540597055518L;

                public Boolean call(Tuple2<Object, Rating[]> t2) throws Exception {
                    boolean ret = false;
                    if (Integer.parseInt(t2._1 + "") == targetId) ret = true;
                    if (targetId == 0) ret = true;
                    return ret;
                }
            }).cache();
            System.out.println("[" + new Date() + "] userRating finished");

            System.out.println("[" + new Date() + "] ratings2 collect start");
            List<Rating> removeRatingList = ratings2.collect();
            System.out.println("[" + new Date() + "] ratings2 collect end");
            System.out.println("[" + new Date() + "] userRatingList start");
            List<Tuple2<Object, Rating[]>> userRatingList = userRating.collect();
            jsc.stop();
            System.out.println("[" + new Date() + "] userRatingList end");


            String outputArr[] = new String[userRatingList.size()];
            String item = "";
            int size = 0;

            System.out.println("[" + new Date() + "] output creating start - " + userRatingList.size());
//		    for(Tuple2<Object, Rating[]> t2 : userRatingList) {
            for (int i = 0; i < userRatingList.size(); i++) {
                if (i % 1000 == 0) {
                    System.out.println("[" + new Date() + "] output is processing - " + i + "/" + userRatingList.size());
                }
                Tuple2<Object, Rating[]> t2 = userRatingList.get(i);
                String userid = t2._1 + "";
                List<Rating> ratList = Arrays.asList(t2._2);
                //System.out.println("\nuserid:"+userid);

                output = "";
                item = "";
                size = 0;
                for (Rating rat : ratList) {
                    boolean check = true;
                    for (Rating rm_rat : removeRatingList) {
                        if (rm_rat.user() == Integer.parseInt(userid) && rm_rat.product() == rat.product()) {
                            check = false;
                            break;
                        }
                    }
                    if (check) {
                        size = size + 1;
                        if (baseMode.equals("item")) {
                            item += "\n{\"itemid\":\"" + rat.product() + "\",";
                        } else { //user
                            item += "\n{\"itemid\":\"" + map.get(rat.product()) + "\",";
                        }
                        item += "\"value\":" + rat.rating() + "},";
                        //System.out.println(rat.product()+" / "+rat.rating());
                    }
                    if (Integer.parseInt(maxResultStr) <= size) {
                        break;
                    }
                }

                if (baseMode.equals("item")) {
                    output += "\"targetid\":" + "\"" + map.get(userid) + "\"";
                } else { //user
                    output += "\"targetid\":" + "\"" + userid + "\"";
                }
//				output += "\"targetid\":" + "\"" + userid + "\"";
                output += ",\n\"size\":" + size;

                if (!item.equals("")) {
                    item = item.substring(1, item.length() - 1);
                    output = output + ",\n\"list\":[\n" + item + "\n]";
                }
                if (!output.equals("")) {
                    outputArr[i] = "{\n" + output + "\n}";
                }

            }
            System.out.println("[" + new Date() + "] output for loop end");


            System.out.println("[" + new Date() + "] return output Arr  for loop start");
            String resultOutput = "";
            for (int i = 0; i < outputArr.length; i++) {
                if (!"".equals(outputArr[i])) {
                    resultOutput += "," + outputArr[i];
                }
            }
            System.out.println("[" + new Date() + "] return output Arr  for loop end");

            if (resultOutput.length() > 0) {
                finaloutput = "{\n\"cfresult\":[\n" + resultOutput.substring(1, resultOutput.length()) + "\n]\n}";
            } else {
                finaloutput = "{\n\"cfresult\":[\n" + "" + "\n]\n}";
            }


        } catch (Exception e) {
            //e.printStackTrace();
            System.err.println(e.toString());
            finaloutput = "{\"exception\":\"" + e.getMessage() + "\"}";

        } finally {
            System.out.println("[" + new Date() + "] finally - finaloutput replace");
            finaloutput = replace(finaloutput, "\n", "");
//			System.out.println(finaloutput);
            System.out.println("[" + new Date() + "] finally - File write start");

            System.out.println("[" + new Date() + "] output file created : " + outputPath);
            File file = new File(outputPath);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write(finaloutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("[" + new Date() + "] finally - File write done");
            System.out.println("[" + new Date() + "] process end");
        }

    }

}