/**
 * @Package PACKAGE_NAME.JavaWordCount
 * @Author lv.zirao
 * @Date 2025/5/28 16:49
 * @description:
 */
import java.util.*;
public class JavaWordCount {
    public static void main(String[] args) {
        String text = "Apache Spark is a fast and general-purpose cluster computing system. It provides high-level " +
                "APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. " +
                "It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, " +
                "MLlib for machine learning, GraphX for graph processing, and Spark Streaming.";
        String[] words = text.split("[^a-zA-Z]+");
        Map<String, Integer> wordCountMap = new HashMap<>();
        for (String word : words) {
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
            }
        }
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(wordCountMap.entrySet());

        entryList.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

        for (int i = 0; i < Math.min(10, entryList.size()); i++) {
            Map.Entry<String, Integer> entry = entryList.get(i);
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }


    }
}
