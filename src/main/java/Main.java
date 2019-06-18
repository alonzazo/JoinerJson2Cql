import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args){
        Cluster cluster = null;
        try {
            String tablename;
            String path1;
            String path2;

            if (args.length == 2){
                tablename = "reviews";
                path1 = args[0];
                path2 = args[1];
            }
            else if (args.length == 3){
                tablename = args[3];
                path1 = args[0];
                path2 = args[1];
            }
            else
                throw new Exception();

            File fileInput1 = new File(path1);
            File fileInput2 = new File(path2);
            ProgressCounter progressCounter = new ProgressCounter(fileInput1.length() + fileInput2.length());

            InputStream inputStream1 = new FileInputStream(path1);
            InputStream inputStream2 = new FileInputStream(path2);
            Scanner scanner1 = new Scanner(inputStream1);
            Scanner scanner2 = new Scanner(inputStream2);

            //File file = new File("out.sql");
            //file.createNewFile();

            //FileWriter fileWriter = new FileWriter("out.sql",true);

            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("54.167.192.105")
                    .build();
            Session session = cluster.connect();                                           // (2)

            JSONParser jsonParser = new JSONParser();
            progressCounter.start();

            while (scanner1.hasNextLine()){

                String lineLeft = scanner1.nextLine();

                JSONObject rowLeft = (JSONObject) jsonParser.parse(lineLeft);

                JSONObject newJSON;

                while (scanner2.hasNextLine()){
                    String lineRight = scanner2.nextLine();

                    JSONObject rowRight = (JSONObject) jsonParser.parse(lineRight);

                    if (Joiner.areJoinable(rowLeft, rowRight)){
                        newJSON = Joiner.join(rowLeft, rowRight);

                        //Inserción en base de datos
                        BuiltStatement insertOne = QueryBuilder.insertInto(tablename).json(newJSON.toJSONString());

                        PreparedStatement preparedStatement = session.prepare(insertOne);

                        ResultSet rs = session.execute(preparedStatement.bind(1));    // (3)

                        progressCounter.advance(lineLeft.length() + lineRight.length());

                    }else {
                        continue;
                    }
                }

                /*fileWriter.append(cqlLine + "\n");
                fileWriter.flush();*/

                /*progressCounter.advance(line.length());*/

            }

            /*fileWriter.close();*/

            progressCounter.finish();

        }catch (Exception e){
            e.printStackTrace();

        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static class ProgressCounter {
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
            printStream.println("This process has been started!");
        }

        public void advance(long quantity){
            currentProgress += quantity;
            report();
        }

        public void setProgress(long progressPoint){
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
            printStream.println("Current progress: " + (currentProgress * 100) / total + "%");
        }
    }
}
