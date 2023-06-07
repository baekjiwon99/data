import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;

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

		PairFunction<String, String, String> fmf = new PairFunction<String, String, String>() {
			public Tuple2<String,String> call(String s){
				
				String[] str = s.split(",");
				String val1 = str[0] + "," + returnDay(str[1]);
				String val2 =  str[3] + "," + str[2];
				return new Tuple2(val1, val2);
			}
		};
		JavaPairRDD<String, String> pair = lines.mapToPair(fmf);
		
		Function2<String, String, String> f2 = new Function2<String, String, String>(){
			public String call(String x, String y){
				String[] word1 = x.split(",");
				String[] word2 = y.split(",");
				int num1 = Integer.parseInt(word1[0]) + Integer.parseInt(word2[0]);
				int num2 = Integer.parseInt(word1[1]) + Integer.parseInt(word2[1]); 
				String value = num1 + "," + num2;
				return value;
			}
		};
		JavaPairRDD<String, String> counts = pair.reduceByKey(f2);
		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
	public static String returnDay(String date){ 
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy"); 
		String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"}; 
		Calendar cal = Calendar.getInstance(); 
		Date getDate; 
		try { 
			getDate = format.parse(date); 
			cal.setTime(getDate); 
			int i = cal.get(Calendar.DAY_OF_WEEK) - 1; 
			return week[i];
		} catch (ParseException e) { 
			e.printStackTrace(); 
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
		return "0";
	}
}
