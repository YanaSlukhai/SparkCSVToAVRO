import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CsvToAvroConverterTest {
    CsvToAvroConverter testConverter = new CsvToAvroConverter();

    @Test
    public void convertCsvFileToAvroTest(){
        testConverter.convertCsvFileToAvro("src/test/resources/destinations.csv", "src/test/results/destinations");
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkCSVToAVROTest")
                .getOrCreate();

        Dataset<Row> avro_file_dataset = spark
                .read()
                .format("com.databricks.spark.avro")
                .option("header", "true")
                .load("src/test/results/destinations");

        Dataset<Row> csv_file_dataset = spark
                .read()
                .option("header", "true")
                .csv("src/test/resources/destinations.csv");

        assertEquals( 0, avro_file_dataset.except(csv_file_dataset).count());
        assertEquals( 0, csv_file_dataset.except(avro_file_dataset).count());
        File fileToDelete = new File("src/test/results");
        try {
            FileUtils.deleteDirectory(fileToDelete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
