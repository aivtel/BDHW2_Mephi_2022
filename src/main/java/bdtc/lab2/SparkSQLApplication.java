package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

/**
 * Count average message.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входная папка, args[1] - выходная папка
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }

        log.info("Application started!");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());

        log.info("===============COUNTING...================");
        JavaRDD<Row> result = MessageCounter.countAvgMessageQuantityByGroup(df, sc);
        log.info("============SAVING FILE TO " + args[1] + " directory============");
        result.saveAsTextFile(args[1]);
    }
}
