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

        JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
		String [] splited = s.split(",");
		String date = splited[1];

		SimpleDateFormat transFormat = new SimpleDateFormat("MM/dd/yyyy");
                  String dayOfWeek = "";
          
                  try {
                    Date dateObj = transFormat.parse(date);
				
                    Calendar cal = Calendar.getInstance() ;
                    cal.setTime(dateObj);
				     
                    int dayNum = cal.get(Calendar.DAY_OF_WEEK) ;
					
                    dayOfWeek = parseToString(dayNum);
                  } catch (Exception e) {}
                    String key = splited[0] + "," + dayOfWeek;
                    String value = splited[3] + "," + splited[2];

                    return new Tuple2(key, value);
                   }
        });

        JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
            public String call(String val1, String val2) {
            
                String[] val1_splt = val1.split(",");
                String[] val2_splt = val2.split(",");

                int trips = Integer.parseInt(val1_splt[0]) + Integer.parseInt(val2_splt[0]);
                int vehicles = Integer.parseInt(val1_splt[1]) + Integer.parseInt(val2_splt[1]);

                return trips + "," + vehicles;
            }
        });

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
