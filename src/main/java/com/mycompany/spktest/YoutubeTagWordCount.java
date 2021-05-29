
package com.mycompany.spktest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class YoutubeTagWordCount {
    
      private static final String Pipe_DELIMITER = "|";
      private static final String  comma_DELIMITER = ",";

    public static void getTagWordCount() throws IOException {
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("tagCounts").setMaster ("local[6]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        //JavaRDD<String> videos = sparkContext.textFile ("src/main/resources/data/USvideos.csv");
        JavaRDD<String> videos = sparkContext.textFile ("USvideos.csv");
 
   
        
        // TRANSFORMATIONS
        JavaRDD<String> tags = videos
                .map (YoutubeTagWordCount::extractTag)
                .filter (StringUtils::isNotBlank);
       // JavaRDD<String>
        JavaRDD<String> words = tags.flatMap (tag -> Arrays.asList (tag
                .toLowerCase ()
                .trim ()
                .replaceAll ("\\p{Punct}", " ")
               .split (" ")).iterator ());
        System.out.println("*****************************************" + words.toString ());
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    private static String extractTag(String videoLine) {
        try {
             String str =  videoLine.split (comma_DELIMITER)[6];
             return  str.split(comma_DELIMITER)[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }
}
