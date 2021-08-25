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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		File tempFile = null;
		String output = "";
		String finaloutput = "";
		String dataMode = "";
		String thresholdStr = "";
		String dataFilePath = "";
		String targetIdStr = "";
		String jsonDataStr = "";

		String masterLocal = "";
		String numStr = "";
		String rankStr = "";
	    String numIterationsStr = "";
		String lambdaStr = "";
		
		String logLevel = "";
		String maxResultStr = "";
		boolean isDebug = false;
		
		try {
			String debug = System.getProperty("debug");
			if(debug == null) throw new Exception();
			if("true".equals(debug)) isDebug = true;
		} catch (Exception e) {
//			isDebug = false;
			isDebug = true;
		}
		try {
			dataMode = args[0];
		} catch (Exception e) {
			dataMode = "json"; // file,json
		}
		if(isDebug) System.out.println("dataMode:"+dataMode);
		try {
			thresholdStr = args[1];
		} catch (Exception e) {
			thresholdStr = "0.9";
		}
		if(isDebug) System.out.println("thresholdStr:"+thresholdStr);
		lambdaStr = thresholdStr;

		try {
			targetIdStr = args[2];
		} catch (Exception e) {
			targetIdStr = "all";
		}
		if(isDebug) System.out.println("targetIdStr:"+targetIdStr);
		try {
			dataFilePath = args[3];
		} catch (Exception e) {
			dataFilePath = "C:\\Users\\Admin\\Desktop\\workspace\\CollaborativeFiltering\\data\\spark.json";
		}
		if(isDebug) System.out.println("dataFilePath:"+dataFilePath);
		try {
			jsonDataStr = args[4];
		} catch (Exception e) {
//			jsonDataStr = "{\"data\": [\"928221,GP251021103,35\",\"928221,GS251017342,33\",\"928221,PI000001565,32\",\"928221,GP251021106,25\",\"928221,PI000003245,23\",\"928221,GP251020590,22\",\"928221,GP251020914,22\",\"928221,GP251021323,21\",\"928221,GP251021325,21\",\"928221,GS251017820,21\",\"942879,GI251024490,41\",\"942879,GI251024341,21\",\"942879,PI000001968,16\",\"942879,PI000000567,14\",\"942879,PI000001485,10\",\"942879,GI251024340,8\",\"942879,PI000001407,8\",\"942879,PI000013359,8\",\"942879,PI000001966,7\",\"942879,PI000001998,7\",\"893739,PI000003344,44\",\"893739,GI251024490,39\",\"893739,PI000012970,29\",\"893739,GI251023924,16\",\"893739,PI000003181,13\",\"893739,GS251017490,12\",\"893739,PI000003365,12\",\"893739,PI000003178,11\",\"893739,GI251024790,10\",\"893739,PI000003075,8\",\"951631,GP251020590,28\",\"951631,GI251020350,23\",\"951631,PI000002330,18\",\"951631,PI000013399,14\",\"951631,GI251018240,11\",\"951631,GS251018422,11\",\"951631,GI251024490,9\",\"951631,GS251018421,9\",\"951631,GI251023891,8\",\"951631,PI000002748,8\",\"949639,GI251018515,9\",\"949639,GS251018679,8\",\"949639,PI000003721,8\",\"949639,GP251021272,7\",\"949639,PI000001509,7\",\"949639,PI000001992,7\",\"949639,PI000002341,7\",\"949639,PI000006564,7\",\"949639,GS251022754,6\",\"949639,PI000001079,6\",\"950350,GI251020535,5\",\"950350,GI251020458,4\",\"950350,GI251020488,4\",\"950350,GP251020409,4\",\"950350,GP251021247,4\",\"950350,PI000005211,4\",\"950350,PI000013410,4\",\"950350,GI251019449,3\",\"950350,GI251021265,3\",\"950350,GP251021276,3\",\"866035,PI000003245,27\",\"866035,PI000003344,22\",\"866035,PI000002371,20\",\"866035,PI000002342,19\",\"866035,GP251020914,18\",\"866035,PI000003074,18\",\"866035,PI000003233,13\",\"866035,PI000000919,10\",\"866035,PI000003072,10\",\"866035,GI251020506,8\",\"899349,PI000004064,12\",\"899349,GS251018389,11\",\"899349,PI000006564,10\",\"899349,GI251024490,9\",\"899349,GI251024340,7\",\"899349,GP251021947,7\",\"899349,GP251024638,7\",\"899349,PI000001380,7\",\"899349,PI000001485,7\",\"899349,PI000003218,7\",\"960618,GI251023925,11\",\"960618,GI251023924,10\",\"960618,PI000013349,9\",\"960618,PI000005693,8\",\"960618,PI000011613,7\",\"960618,PI000003329,6\",\"960618,PI000013347,6\",\"960618,GO251021728,5\",\"960618,GP251019292,5\",\"960618,PI000002435,5\",\"909632,GP251021755,11\",\"909632,GI251018885,8\",\"909632,GP251021754,8\",\"909632,PI000002710,8\",\"909632,PI000003133,8\",\"909632,GI251020348,7\",\"909632,GI251024490,7\",\"909632,PI000000316,7\",\"909632,PI000000737,6\",\"909632,PI000001509,6\"]}";
			jsonDataStr = "{\"data\": [\"928221,GP251021103,35,1\",\"928221,GS251017342,33,2\",\"928221,PI000001565,32,3\",\"928221,GP251021106,25,4\",\"928221,PI000003245,23,5\",\"928221,GP251020590,22,6\",\"928221,GP251020914,22,7\",\"928221,GP251021323,21,8\",\"928221,GP251021325,21,9\",\"928221,GS251017820,21,10\",\"942879,GI251024490,41,11\",\"942879,GI251024341,21,12\",\"942879,PI000001968,16,13\",\"942879,PI000000567,14,14\",\"942879,PI000001485,10,15\",\"942879,GI251024340,8,16\",\"942879,PI000001407,8,17\",\"942879,PI000013359,8,18\",\"942879,PI000001966,7,19\",\"942879,PI000001998,7,20\",\"893739,PI000003344,44,21\",\"893739,GI251024490,39,22\",\"893739,PI000012970,29,23\",\"893739,GI251023924,16,24\",\"893739,PI000003181,13,25\",\"893739,GS251017490,12,26\",\"893739,PI000003365,12,27\",\"893739,PI000003178,11,28\",\"893739,GI251024790,10,29\",\"893739,PI000003075,8,30\",\"951631,GP251020590,28,31\",\"951631,GI251020350,23,32\",\"951631,PI000002330,18,33\",\"951631,PI000013399,14,34\",\"951631,GI251018240,11,35\",\"951631,GS251018422,11,36\",\"951631,GI251024490,9,37\",\"951631,GS251018421,9,38\",\"951631,GI251023891,8,39\",\"951631,PI000002748,8,40\",\"949639,GI251018515,9,41\",\"949639,GS251018679,8,42\",\"949639,PI000003721,8,43\",\"949639,GP251021272,7,44\",\"949639,PI000001509,7,45\",\"949639,PI000001992,7,46\",\"949639,PI000002341,7,47\",\"949639,PI000006564,7,48\",\"949639,GS251022754,6,49\",\"949639,PI000001079,6,50\",\"950350,GI251020535,5,51\",\"950350,GI251020458,4,52\",\"950350,GI251020488,4,53\",\"950350,GP251020409,4,54\",\"950350,GP251021247,4,55\",\"950350,PI000005211,4,56\",\"950350,PI000013410,4,57\",\"950350,GI251019449,3,58\",\"950350,GI251021265,3,59\",\"950350,GP251021276,3,60\",\"866035,PI000003245,27,61\",\"866035,PI000003344,22,62\",\"866035,PI000002371,20,63\",\"866035,PI000002342,19,64\",\"866035,GP251020914,18,65\",\"866035,PI000003074,18,66\",\"866035,PI000003233,13,67\",\"866035,PI000000919,10,68\",\"866035,PI000003072,10,69\",\"866035,GI251020506,8,70\",\"899349,PI000004064,12,71\",\"899349,GS251018389,11,72\",\"899349,PI000006564,10,73\",\"899349,GI251024490,9,74\",\"899349,GI251024340,7,75\",\"899349,GP251021947,7,76\",\"899349,GP251024638,7,77\",\"899349,PI000001380,7,78\",\"899349,PI000001485,7,79\",\"899349,PI000003218,7,80\",\"960618,GI251023925,11,81\",\"960618,GI251023924,10,82\",\"960618,PI000013349,9,83\",\"960618,PI000005693,8,84\",\"960618,PI000011613,7,85\",\"960618,PI000003329,6,86\",\"960618,PI000013347,6,87\",\"960618,GO251021728,5,88\",\"960618,GP251019292,5,89\",\"960618,PI000002435,5,90\",\"909632,GP251021755,11,91\",\"909632,GI251018885,8,92\",\"909632,GP251021754,8,93\",\"909632,PI000002710,8,94\",\"909632,PI000003133,8,95\",\"909632,GI251020348,7,96\",\"909632,GI251024490,7,97\",\"909632,PI000000316,7,98\",\"909632,PI000000737,6,99\",\"909632,PI000001509,6,100\",]}";
		}
		if(isDebug) System.out.println("jsonDataStr:"+jsonDataStr);
	  
		try {
			numStr = System.getProperty("num");
			if(numStr == null) throw new Exception();
		} catch (Exception e) {
			numStr = "100";
		}
		if(isDebug) System.out.println("numStr:"+numStr);
		try {
			rankStr = System.getProperty("rank");
			if(rankStr == null) throw new Exception();
		} catch (Exception e) {
			rankStr = "10";
		}
		if(isDebug) System.out.println("rankStr:"+rankStr);
		try {
			numIterationsStr = System.getProperty("numIterations");
			if(numIterationsStr == null) throw new Exception();
		} catch (Exception e) {
			numIterationsStr = "10";
		}
		if(isDebug) System.out.println("numIterationsStr:"+numIterationsStr);
		try {
			maxResultStr = System.getProperty("maxResultStr");
			if(maxResultStr == null) throw new Exception();
		} catch (Exception e) {
			maxResultStr = "30";
		}
		if(isDebug) System.out.println("maxResultStr:"+maxResultStr);
		try {
			System.setProperty("hadoop.home.dir", args[5]);
		} catch (Exception e) {
			//System.setProperty("hadoop.home.dir", "C:\\appl\\hadoop-winutils-2.6.0");
//			System.setProperty("hadoop.home.dir", "/home/sop/test/hadoop-winutils-2.6.0");
		}
		//System.setProperty("hadoop.home.dir", "C:\\appl\\hadoop-winutils-2.6.0");

		try {
			masterLocal = System.getProperty("masterLocal");
			if(masterLocal == null) throw new Exception();
		} catch (Exception e) {
			masterLocal = "local[4]";
		}
		if(isDebug) System.out.println("masterLocal:"+masterLocal);
		try {
			logLevel = System.getProperty("logLevel");
			if(logLevel == null) throw new Exception();
		} catch (Exception e) {
			logLevel = "ERROR";
		}

		if("ERROR".equals(logLevel)) {
			Logger.getLogger("apache").setLevel(Level.ERROR); 
			Logger.getLogger("com").setLevel(Level.ERROR); 
			Logger.getLogger("org").setLevel(Level.ERROR); 
		}else 
		if("INFO".equals(logLevel)) {
			Logger.getLogger("apache").setLevel(Level.INFO); 
			Logger.getLogger("com").setLevel(Level.INFO); 
			Logger.getLogger("org").setLevel(Level.INFO); 
		}else 
		if("WARN".equals(logLevel)) {
			Logger.getLogger("apache").setLevel(Level.WARN); 
			Logger.getLogger("com").setLevel(Level.WARN); 
			Logger.getLogger("org").setLevel(Level.WARN); 
		}
		
		try {
			if ("file".equals(dataMode.toLowerCase().trim())) {
				//datamodel = new FileDataModel(new File(dataFilePath));
				
			} else if ("json".equals(dataMode.toLowerCase().trim())) {
				tempFile = File.createTempFile("tempFile_", ".tmp");
				java.io.BufferedWriter bw = new java.io.BufferedWriter(new java.io.FileWriter(tempFile));

				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonDataStr);
				JSONArray jsonArray = (JSONArray) jsonObject.get("data");

				String data = "";
				for (int i = 0; i < jsonArray.size(); i++) {
					data += jsonArray.get(i) + "\n";
				}

				bw.write(data);
				bw.close();
				dataFilePath = tempFile.getAbsolutePath();
			}
			
			if("all".equals(targetIdStr)) targetIdStr = "0";

			SparkConf conf = new SparkConf().setAppName("CF-Spark MLlib");
			if(!"".equals(masterLocal))conf.setMaster(masterLocal);
			JavaSparkContext jsc = new JavaSparkContext(conf);

			//--------------------------------------------
			int targetId = Integer.parseInt(targetIdStr);
			int num = Integer.parseInt(numStr);
			int rank = Integer.parseInt(rankStr);
			int numIterations = Integer.parseInt(numIterationsStr);
			double lambda = Double.parseDouble(lambdaStr);
			//--------------------------------------------
			
			// Load and parse the data
			String path = dataFilePath;

			// 임의로 데이터 파일 경로 넣어줌
//			dataFilePath = "C:\\Users\\Admin\\AppData\\Local\\Temp\\test.tmp";
			dataFilePath = "/home/sop/test/data/spark-7313183.tmp";
			JavaRDD<String> data = jsc.textFile(dataFilePath);
			System.out.println("data : " + dataFilePath);


			Map<Integer, String> map = new HashMap<>();
			BufferedReader reader = new BufferedReader(
//					new FileReader("C:\\Users\\Admin\\AppData\\Local\\Temp\\tempFile_8715259122777345222.tmp")
//					new FileReader("C:\\Users\\Admin\\AppData\\Local\\Temp\\spark-7313183.tmp")
					new FileReader("/home/sop/test/data/spark-7313183.tmp")
			);
			String str;
			while ((str = reader.readLine()) != null) {
				int key = Integer.parseInt(str.split(",")[3]);
				String value = str.split(",")[1];
//				System.out.println(key + " :: " + value);
				map.put(key, value);
			}
			reader.close();



			// Mapping Table Create
			JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
				private static final long serialVersionUID = 571408671101770835L;
				public Rating call(String s) {
					String[] sarray = s.split("[,]");
					return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[3]),
							Double.parseDouble(sarray[2]));
				};
			}).cache();


		    // Build the recommendation model using ALS
			MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, lambda);
			System.out.println("map size : " + map.size());
		    JavaRDD<Rating> ratings2 = ratings.filter(new Function<Rating, Boolean>() {
				private static final long serialVersionUID = 8900885706131295795L;
				public Boolean call(Rating rating) {
		    		Boolean ret = false;
		    		if(rating.user() == targetId) ret = true;
		    		if(targetId == 0) ret = true;
		            return ret;
		         }
		    }).cache();

		    JavaRDD<Tuple2<Object, Rating[]>> userRating = model.recommendProductsForUsers(num).toJavaRDD().filter(new Function<Tuple2<Object, Rating[]>, Boolean>() {
				private static final long serialVersionUID = 2333498540597055518L;
				public Boolean call(Tuple2<Object, Rating[]> t2) throws Exception {
		    		Boolean ret = false;
		    		if(Integer.parseInt(t2._1+"") == targetId) ret = true;
		    		if(targetId == 0) ret = true;
		    		return ret;
		        }
		    }).cache();

		    List<Rating> removeRatingList = ratings2.collect();
		    List<Tuple2<Object, Rating[]>> userRatingList = userRating.collect();
		    jsc.stop();


		    String outputArr[] = new String[userRatingList.size()];
		    String item = "";
		    int size = 0;

		    //for(Tuple2<Object, Rating[]> t2 : userRatingList) {
		    for(int i=0;i<userRatingList.size();i++) {
		    	Tuple2<Object, Rating[]> t2 = userRatingList.get(i);
		    	String userid = t2._1+"";
		        List<Rating> ratList = Arrays.asList(t2._2);
		        //System.out.println("\nuserid:"+userid);

				output = "";
				item = "";
				size = 0;
		        for(Rating rat : ratList) {
		        	boolean check = true;
		        	for(Rating rm_rat : removeRatingList) {
		        		if(rm_rat.user() == Integer.parseInt(userid) && rm_rat.product() == rat.product()) {
		        			check = false;
		        			break;
		        		}
		        	}
		        	if(check) {
		        		size = size + 1;
						item += "\n{\"itemid\":\"" + map.get(rat.product()) + "\",";
						item += "\"value\":" + rat.rating() + "},";
		        		//System.out.println(rat.product()+" / "+rat.rating());
		        	}
		        	if(Integer.parseInt(maxResultStr) <= size) {
		        		break;
		        	}
		        }

				output += "\"targetid\":" + "\"" + userid + "\"";
				output += ",\n\"size\":" + size;

				if (!item.equals("")) {
					item = item.substring(1, item.length() - 1);
					output = output + ",\n\"list\":[\n" + item + "\n]";
				}
				if (!output.equals("")) {
					outputArr[i] = "{\n" + output + "\n}";
				}

		    }

			String resultOutput = "";
			for(int i = 0; i < outputArr.length; i++) {
				if(!"".equals(outputArr[i])) {
					resultOutput += ","+outputArr[i];
				}
			}

			if(resultOutput.length() > 0) {
				finaloutput = "{\n\"cfresult\":[\n" + resultOutput.substring(1,resultOutput.length()) + "\n]\n}";
			}else {
				finaloutput = "{\n\"cfresult\":[\n" + "" + "\n]\n}";
			}

		    
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println(e.toString());
			finaloutput = "{\"exception\":\"" + e.getMessage() + "\"}";

		} finally {
			finaloutput = replace(finaloutput,"\n","");
			System.out.println(finaloutput);


			File file = new File("C:\\Users\\Admin\\Desktop\\workspace\\CollaborativeFiltering\\output\\output.json");
//			String str = "Hello world!";

			try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
				writer.write(finaloutput);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (tempFile != null) {
				// FileDeleteStrategy.FORCE.delete(tempFile);
				// System.out.println("File deleted");
			}
		}		
	
  }

}