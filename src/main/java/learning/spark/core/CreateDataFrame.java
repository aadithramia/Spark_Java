package learning.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class CreateDataFrame {

	public static void main(String[] args) {
		

		  List<Row> rows = Arrays.asList(
                    RowFactory.create(1, "Alice", 12),
                    RowFactory.create(2, "Bob", 23),
                    RowFactory.create(3, "Cathy", 67),
                    RowFactory.create(4, "Bob", 45),
                    RowFactory.create(5, "Elena", null)
            );

            // Define schema
            StructType schema = DataTypes.createStructType(new StructField[]{                    
					DataTypes.createStructField("ID", DataTypes.IntegerType, false),
                    DataTypes.createStructField("Name", DataTypes.StringType, false),
                    DataTypes.createStructField("Age", DataTypes.IntegerType, true)
            });

        
			String log4jConfigPath = "file:/C:/Users/shravanr/learning/spark/spark_java/log4j.properties";

			System.setProperty("log4j.configuration", log4jConfigPath);

			SparkConf conf = new SparkConf()
			.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=" + log4jConfigPath) //-> this does not work locally. this works 
			;

		
		 // Using try-with-resources for SparkSession
		 try (SparkSession spark = SparkSession.builder()
		 .appName("CreateDF")
		 .master("local[*]")
		 .config(conf)
		 .config("spark.sql.shuffle.partitions", "1")
		 .getOrCreate()) {

			spark.sparkContext().setLogLevel("ERROR");
			JavaRDD<Row> rdd =	new JavaSparkContext( spark.sparkContext()).parallelize(rows, 2);
			Dataset<Row> df = spark.createDataFrame(rdd, schema);
			df.show();

		 } // SparkSession is closed automatically

		


	}
}
