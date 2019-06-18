import org.json.simple.JSONObject;

public class Joiner {

    public static boolean areJoinable(JSONObject leftRow, JSONObject rightRow){
        return leftRow.get("asin").equals(rightRow.get("asin")) ? true: false;
    }

    public static boolean areJoinable(JSONObject leftRow, JSONObject rightRow, String inLeftColumn, String inRightColumn){
        return leftRow.get(inLeftColumn).equals(rightRow.get(inRightColumn)) ? true: false;
    }

    public static JSONObject join(JSONObject leftRow, JSONObject rightRow){
        for (Object key: rightRow.keySet())
            if (!leftRow.keySet().contains(key))
                leftRow.put(key, rightRow.get(key));
        return leftRow;
    }
}
