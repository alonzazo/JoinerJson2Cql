import com.datastax.driver.core.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static java.time.temporal.ChronoUnit.SECONDS;

enum Condition { RIGHT_INNER_JOIN, LEFT_INNER_JOIN, FULL_OUTER_JOIN }

public class MainHash {


    public static void main(String[] args){
        Cluster cluster = null;
        Condition condition = null;
        try {
            String tableToName;
            String path1;
            String table2Name;
            String partitionKey;

            if (args.length == 2){
                tableToName = "reviewsJoined";
                path1 = args[0];
                table2Name = args[1];
                partitionKey = "asin";
            }
            else if (args.length == 3){
                tableToName = "reviewsJoined";
                path1 = args[0];
                table2Name = args[1];
                partitionKey = "asin";

                if (args[2].equals("--OUTER")){
                    condition = Condition.FULL_OUTER_JOIN;
                }
                else if (args[2].equals("--LEFT")){
                    condition = Condition.LEFT_INNER_JOIN;
                }
                else if (args[2].equals("--RIGHT")){
                    condition = Condition.RIGHT_INNER_JOIN;
                }
                else {
                    tableToName = args[2];
                }
            }
            else if (args.length == 4){
                path1 = args[0];
                table2Name = args[1];
                tableToName = args[2];
                partitionKey = "asin";

                if (args[3].equals("--OUTER")){
                    condition = Condition.FULL_OUTER_JOIN;
                }
                else if (args[3].equals("--LEFT")){
                    condition = Condition.LEFT_INNER_JOIN;
                }
                else if (args[3].equals("--RIGHT")){
                    condition = Condition.RIGHT_INNER_JOIN;
                }
                else {
                    tableToName = args[3];
                }
            }
            else if (args.length == 5) {
                path1 = args[0];
                table2Name = args[1];
                partitionKey = args[2];
                tableToName = args[3];

                if (args[4].equals("--OUTER")){
                    condition = Condition.FULL_OUTER_JOIN;
                }
                else if (args[4].equals("--LEFT")){
                    condition = Condition.LEFT_INNER_JOIN;
                }
                else if (args[4].equals("--RIGHT")){
                    condition = Condition.RIGHT_INNER_JOIN;
                }
                else {
                    tableToName = args[4];
                }
            }
            else
                throw new Exception();

            File fileInput1 = new File(path1);
            ProgressCounter progressCounter = new ProgressCounter(fileInput1.length());

            InputStream inputStream1 = new FileInputStream(path1);

            Scanner scanner1 = new Scanner(inputStream1);

            cluster = Cluster.builder()                                                    // (1)
                    //.addContactPoint("3.92.232.169")
                    .addContactPoint("localhost")
                    .build();
            Session session = cluster.connect("scyllaproject");                                           // (2)

            JSONParser jsonParser = new JSONParser();
            progressCounter.start();

            boolean leftHasBeenInsertedAtLeastOne = false;
            Set<String> rightHaveNotBeenAssigned = new HashSet<String>();

            int i = 0;
            while (scanner1.hasNextLine()){

                String lineLeft = scanner1.nextLine();

                JSONObject rowLeft = (JSONObject) jsonParser.parse(lineLeft);

                ResultSet rs = session.execute("SELECT JSON year,reviewerID, asin, toUnixTimestamp(unixReviewTime), helpful, overall, reviewerName, reviewText, reviewTime, summary FROM " + table2Name + " WHERE " + partitionKey + " = '" + rowLeft.get(partitionKey).toString() + "'");
                for (Row row : rs) {
                    JSONObject newJSON = (JSONObject) jsonParser.parse(row.getString(0));

                    newJSON = Joiner.join(rowLeft, newJSON);

                    String query = convertJsonToQuery(newJSON, tableToName);

                    //session.execute(query);

                    leftHasBeenInsertedAtLeastOne = true;
                    rightHaveNotBeenAssigned.add(newJSON.get(partitionKey).toString());
                }

                if (i == 2)
                    break;

                i++;
                /*InputStream inputStream2 = new FileInputStream(path2);

                Scanner scanner2 = new Scanner(inputStream2);


                while (scanner2.hasNextLine()){

                    String lineRight = scanner2.nextLine();

                    JSONObject rowRight = (JSONObject) jsonParser.parse(lineRight);

                    rightHaveNotBeenAssigned.add(rowRight);

                    if (Joiner.areJoinable(rowLeft, rowRight)){

                        newJSON = Joiner.join(rowLeft, rowRight);

                        String query = convertJsonToQuery(newJSON, tablename);

                        session.execute(query);

                        leftHasBeenInsertedAtLeastOne = true;
                        rightHaveNotBeenAssigned.remove(rowRight);

                    }
                }*/

                if (!leftHasBeenInsertedAtLeastOne && (condition == Condition.LEFT_INNER_JOIN || condition == Condition.FULL_OUTER_JOIN))
                {
                    //Insertamos el dato de la izquierda con derecha en nulo
                    String query = convertJsonToQuery(rowLeft, tableToName);

                    session.execute(query);
                }

               // scanner2.close();

                progressCounter.advance(lineLeft.length());

            }

            if (!rightHaveNotBeenAssigned.isEmpty() && (condition == Condition.RIGHT_INNER_JOIN || condition == Condition.FULL_OUTER_JOIN)){
                String query = "SELECT JSON year,reviewerID, asin, toUnixTimestamp(unixReviewTime), helpful, overall, reviewerName, reviewText, reviewTime, summary FROM " + table2Name + " WHERE ";

                for (String rightKeyElement: rightHaveNotBeenAssigned){
                    query += partitionKey + " != '" + rightKeyElement + "' AND ";
                }
                query = query.substring(0, query.length() - 4) + ";";

                //Insertamos el dato de la derecha con izquierda en nulo

                ResultSet rs = session.execute(query);
                for (Row row : rs){
                    JSONObject newJSON = (JSONObject) jsonParser.parse(row.getString(0));

                    String queryNew = convertJsonToQuery(newJSON, tableToName);

                    session.execute(queryNew);
                }
            }

            /*fileWriter.close();*/

            progressCounter.finish();

        }catch (Exception e){
            e.printStackTrace();

        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static String convertJsonToQuery(JSONObject jsonObject, String tableName){
        String reviewTime = jsonObject.get("reviewtime").toString();
        String year = reviewTime.substring(reviewTime.length()-4);

        jsonObject.put("year", year);

        jsonObject.put("unixreviewtime", jsonObject.get("tounixtimestamp(unixreviewtime)"));
        jsonObject.remove("tounixtimestamp(unixreviewtime)");

        jsonObject.put("related","");

        JSONArray categoriesArray = (JSONArray) jsonObject.get("categories");
        System.out.println(((JSONArray) jsonObject.get("categories")).toJSONString());
        JSONArray categoriesExtended = new JSONArray();
        for (int i = 0; i < categoriesArray.size(); i++){
            if (categoriesArray.get(i) instanceof JSONArray) {
                for (Object element : (JSONArray) categoriesArray.get(i))
                    categoriesExtended.add(element);
            }else {
                categoriesExtended.add(categoriesArray.get(i));
            }
        }

        if (!jsonObject.containsKey("brand"))
            jsonObject.put("brand","");

        jsonObject.put("categories",categoriesExtended);

        return "INSERT INTO " + tableName + " JSON '" + jsonObject.toJSONString().replace("\'","\\\"") + "';";
    }

    private static class ProgressCounter {
        private long lastProgress;
        private Instant lastTimestamp;
        private long currentProgress;
        private long total;
        private PrintStream printStream;

        public ProgressCounter(long total){
            currentProgress = 0;
            this.total = total;
            printStream = System.out;
        }

        public ProgressCounter(long total, PrintStream printStream){
            currentProgress = 0;
            this.total = total;
            this.printStream = printStream;
        }

        public void start(){
            lastProgress = 0;
            lastTimestamp = Instant.now();
            printStream.println("This process has been started!");
        }

        public void advance(long quantity){
            lastProgress = currentProgress;
            currentProgress += quantity;
            report();
        }

        public void setProgress(long progressPoint){
            lastProgress = currentProgress;
            currentProgress = progressPoint;
            report();
        }

        public void setTotal(long total){
            this.total = total;
            report();
        }

        public void finish(){
            printStream.println("The process has been finished!");
        }

        private void report(){
            double porcentageCurrentProgress = (currentProgress * 100.0) / total;
            double estimatedTimeAproximation =  (100.0-porcentageCurrentProgress) / porcentagePerSecond();
            Duration duration = Duration.ofSeconds(Math.round(estimatedTimeAproximation));
            printStream.println("Current progress: " + (currentProgress * 100) / total + "% " + currentProgress + " B /" + total + " B ETA: " + formatDuration(duration));
        }

        private double porcentagePerSecond(){
            Instant currentTimestamp = Instant.now();
            double deltaProgress = (currentProgress  - lastProgress) * 100.0 / total;
            double deltaTime = lastTimestamp.until(currentTimestamp, SECONDS);
            lastTimestamp = currentTimestamp;
            return deltaProgress / deltaTime;
        }

        private String formatDuration(Duration duration) {
            long seconds = duration.getSeconds();
            long absSeconds = Math.abs(seconds);
            String positive = String.format(
                    "%d:%02d:%02d",
                    absSeconds / 3600,
                    (absSeconds % 3600) / 60,
                    absSeconds % 60);
            return seconds < 0 ? "-" + positive : positive;
        }
    }
}
