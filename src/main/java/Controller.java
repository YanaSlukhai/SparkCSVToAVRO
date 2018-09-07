
import java.util.HashMap;
import java.util.Map;

public class Controller {

    public static void main(String[] args){
        Map<String, String> filesToConvert = new HashMap<String, String>();
        filesToConvert.put("hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/destinations.csv", "hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/avro/destinations");
        filesToConvert.put("hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/sample_submission.csv", "hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/avro/sample_submission");
        filesToConvert.put("hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/test.csv", "hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/avro/test");
        filesToConvert.put("hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/train.csv", "hdfs://sandbox-hdp.hortonworks.com/tmp/expedia_hotels/avro/train");

        CsvToAvroConverter converter = new CsvToAvroConverter();
        filesToConvert
                .entrySet()
                .stream()
                .forEach( e-> converter.convertCsvFileToAvro(e.getKey(), e.getValue()));
    }
}

