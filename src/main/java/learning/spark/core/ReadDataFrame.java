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


public class ReadDataFrame {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf()
                .setAppName("ReadDF")
                .set("spark.sql.shuffle.partitions", "2");
		
		try (SparkSession spark = SparkSession.builder()
		.config(conf)
		.master("local[*]")
		.getOrCreate()) {
		
			// Spark operations
			Dataset<Row> df = spark.read()
					.format("csv")
					.option("delimiter", "\t")
					.option("encoding", "UTF-8")
					.option("inferSchema", "true")
					.option("header", "true")
					.load("C:/Users/shravanr/learning/spark/spark_java/data/input_dir/data.tsv");
					


			df.createOrReplaceGlobalTempView("df");
			Dataset<Row> partitioned = df.repartition(2);
			Dataset<Row> avgSalaryByAge = partitioned.filter("Age > 25").groupBy("Department").count();
			
			avgSalaryByAge.show();
		
			// Pause for debugging (optional)
			System.out.println("Press Enter to exit...");
			try {
				System.in.read(); // IOException will propagate to the outer try block
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
}
