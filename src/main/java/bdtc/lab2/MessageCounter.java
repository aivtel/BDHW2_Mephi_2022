package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import java.util.*;
import org.json.*;

@AllArgsConstructor
@Slf4j
public class MessageCounter {

/**
    * загружаем джейсон со словарем какой пользователь к какой группе относится. 
    * json предварительно генерируем с помощью generate.py в корневой папке
 */

    public static JSONArray loadJsonFromFile() throws Exception {
        String pathToJson = "/root/lab2/dict.json";
        File f = new File(pathToJson);
        if (f.exists()){
            InputStream is = new FileInputStream(pathToJson);
            String jsonTxt = IOUtils.toString(is, "UTF-8");
            JSONArray json = new JSONArray(jsonTxt);
            return json;
        }
        return null;
    }

    /**
        * Из загруженного джейсона формируем список
     */

    public static ArrayList<DictEntity> readFromJsonArray(JSONArray dictJsonArray) {
        ArrayList<DictEntity> userGroups = new ArrayList<DictEntity>();
        for(int i = 0; i < dictJsonArray.length(); i++) {
            JSONObject dictItem = dictJsonArray.getJSONObject(i);
            String userName = dictItem.getString("user");
            String groupName = dictItem.getString("group");
            DictEntity entity = new DictEntity();
            entity.setUserName(userName);
            entity.setGroupName(groupName);
            userGroups.add(entity);
        }
        return userGroups;
    }

/**
    * ищем группу пользователя из списка по совпадению его имени
 */
    public static String findGroupNameByUserName(ArrayList<DictEntity> listDicts, String userName) {
        for(DictEntity entity: listDicts) {
            String name = entity.getUserName();
            if (name.equals(userName)) {
                String groupName = entity.getGroupName();
                return groupName;
            }
        }
        return "";
    }

    public static JavaRDD<Row> countAvgMessageQuantityByGroup(Dataset<String> inputDataset, SparkSession sc) throws Exception {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        
        JSONArray dictJson = loadJsonFromFile();
        ArrayList<DictEntity> dictWitGroups = readFromJsonArray(dictJson);

/**
    * Составляем первую таблицу из сообщений, где в записи также будет фигугировать имя группы
 */
        Dataset<MessageRecord> messageRecordDataset = words.map(s -> {
                    String[] recordFields = s.split(";");
                    String currentUserName = recordFields[0].substring(1);
                    String messageText = recordFields[3].substring(0, recordFields[3].length() - 1);
                    String groupName  = findGroupNameByUserName(dictWitGroups,currentUserName);
                    return new MessageRecord(currentUserName, recordFields[1], recordFields[2], messageText, groupName);
                }, Encoders.bean(MessageRecord.class))
                .coalesce(1);

        messageRecordDataset.show();

        /**
         * Считаем количество пользователей в группах в отдельную таблицу
         */
        Dataset<DictEntity> dataDs = sc.createDataset(dictWitGroups, Encoders.bean(DictEntity.class));
        Dataset<Row> countUsersTable = dataDs.groupBy("groupName").count().toDF("groupName", "count");
        log.info("===========COUNTED USERS TABLE===========");
        countUsersTable.show();

        /**
         * Считаем количество сообщений в отдельную таблицу
         */
        Dataset<Row> countMessageTable = messageRecordDataset.groupBy("groupName")
                .count().as("countMess")
                .toDF("groupName", "countMess")
                .sort(functions.asc("groupName"));
        log.info("===========COUNTED MESSAGE TABLE===========");
        countMessageTable.show();


        /**
            * Джой ним таблицы по имени группы
         */
        Dataset<Row> joined = countUsersTable.join(countMessageTable, "groupName");
        log.info("===========JOINED TABLES=========== ");
        joined.show();

        /**
            * Делим количество сообщений на количество пользователей в группе в новую, итоговую таблицу. 
         */

        Dataset<Row> finalDf = joined.withColumn("ratio", functions.col("countMess").divide(functions.col("count")));
        log.info("===========RESULT=========== ");
        finalDf.show();
        return finalDf.toJavaRDD();
    }
}
