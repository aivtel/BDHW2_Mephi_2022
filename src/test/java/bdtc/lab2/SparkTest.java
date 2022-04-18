package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.MessageCounter.countAvgMessageQuantityByGroup;
import static bdtc.lab2.MessageCounter.findGroupNameByUserName;

public class SparkTest {

    final String testString1 = "User_42;User_585;2022-06-18 05:13:50;Wow\n";
    final String testString11 = "User_42;User_585;2022-06-18 05:13:50;Wow again\n";
    final String testString2 = "User_381;User_522;2022-05-05 05:40:23;Bye-bye\n";
    final String testString3 = "User_638;User_982;2022-03-06 11:55:26;Awesome\n";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneLog() throws Exception {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1));
        Dataset<String> input = ss.createDataset(dudu.rdd(), Encoders.STRING());
        JavaRDD<Row> result = countAvgMessageQuantityByGroup(input, ss);
        List<Row> rowList = result.collect();
        assert rowList.iterator().next().getDouble(3) == 0.03225806451612903;
        assert rowList.iterator().next().getString(0).equals("group_27");
    }

    @Test
    public void testTwoLogsSameTime() throws Exception {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString11));
        Dataset<String> input = ss.createDataset(dudu.rdd(), Encoders.STRING());
        JavaRDD<Row> result = countAvgMessageQuantityByGroup(input, ss);
        List<Row> rowList = result.collect();
        assert rowList.iterator().next().getDouble(3) == 0.06451612903225806;
        assert rowList.iterator().next().getString(0).equals("group_27");
    }

    @Test
    public void testTwoLogsDifferentTime() throws Exception {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString11, testString2));
        Dataset<String> input = ss.createDataset(dudu.rdd(), Encoders.STRING());
        JavaRDD<Row> result = countAvgMessageQuantityByGroup(input, ss);
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getDouble(3) == 0.06451612903225806;
        assert firstRow.getString(0).equals("group_27");

        assert secondRow.getDouble(3) == 0.034482758620689655;
        assert secondRow.getString(0).equals("group_3");
    }

    @Test
    public void testFindGroup() {
        ArrayList<DictEntity> userGroups = new ArrayList<DictEntity>();
        DictEntity entity = new DictEntity();
        entity.setUserName("Anton");
        entity.setGroupName("M20-V01");
        userGroups.add(entity);
        String result = findGroupNameByUserName(userGroups, "Anton");
        assert result.equals("M20-V01");
    }

    @Test
    public void testThreeLogs() throws Exception {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString11, testString2, testString3));
        Dataset<String> input = ss.createDataset(dudu.rdd(), Encoders.STRING());
        JavaRDD<Row> result = countAvgMessageQuantityByGroup(input, ss);
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getDouble(3) == 0.03125;
        assert firstRow.getString(0).equals("group_17");
        assert secondRow.getDouble(3) == 0.06451612903225806;
        assert secondRow.getString(0).equals("group_27");
        assert thirdRow.getDouble(3) == 0.034482758620689655;
        assert thirdRow.getString(0).equals("group_3");
    }

}
