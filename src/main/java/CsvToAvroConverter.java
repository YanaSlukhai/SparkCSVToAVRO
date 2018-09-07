import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class CsvToAvroConverter {

    public void convertCsvFileToAvro(String csvPath, String avroPath){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkCSVToAVRO")
                .getOrCreate();

        Dataset<Row> csv_file_dataset = spark
                .read()
                .option("header", "true")
                .csv(csvPath);

        csv_file_dataset.write()
                .format("com.databricks.spark.avro")
                .option("header", "true")
                .save(avroPath);
    }


}
