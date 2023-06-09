import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import java.util.regex.Pattern;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;

public final class UBERStudent20190967 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20190967 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20190967")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
		String[] values = s.split(",");
            	StringTokenizer itr = new StringTokenizer(values[1], "/");
		int year = 0;
		int month = 0;
		String date = "";
		int day = 0;
		
            	while (itr.hasMoreTokens()) {
            		month = Integer.parseInt(itr.nextToken());
            		day = Integer.parseInt(itr.nextToken());
            		year = Integer.parseInt(itr.nextToken());

            	}
		    
            	LocalDate localDate = LocalDate.of(year, month, day);
		DayOfWeek dayOfWeek = localDate.getDayOfWeek();
		date = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();
		    
		if (date.equals("THU")) date = "THR";
		
		String key = values[0] + "," + date;
		String value = values[3] + "," + values[2];
	
                return new Tuple2(key, value);
            }
        };
	    
        JavaPairRDD<String, String> pair = lines.mapToPair(pf);
	    
        Function2<String, String, String> f2 = new Function2<String, String, String>() {
            public String call(String x, String y) {
            	String[] v1 = x.split(",");
            	String[] v2 = y.split(",");
            	
            	int trips = Integer.parseInt(v1[0]) + Integer.parseInt(v2[0]);
            	int vehicles = Integer.parseInt(v1[1]) + Integer.parseInt(v2[1]);
            
                return trips + "," + vehicles;
            }
        };
        JavaPairRDD<String, String> reduce = pair.reduceByKey(f2);
        
        JavaRDD<String> result = reduce.map(x -> x._1 + " " + x._2);

        result.saveAsTextFile(args[1]);
        spark.stop();
    }
}
