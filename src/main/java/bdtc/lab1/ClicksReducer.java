package bdtc.lab1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClicksReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        String temperature = getTemperature(sum); // функция, которая возвращает температуру по количеству нажатий
        context.write(new Text(key.toString() + sum + " (" + temperature + ")"), new IntWritable(sum));
    }
    // Функция, которая возвращает температуру по количеству кликов
    private String getTemperature(int clicks) {
        // Загружаем справочник температур из файла в память
        Map<String, String> tempMap = new HashMap<>();

        tempMap.put("0-9", "Низкая");
        tempMap.put("10-99", "Средняя");
        tempMap.put("100+", "Высокая");

        // Определяем температуру по количеству кликов
        if (clicks < 10) {
            return tempMap.get("0-9");
        } else if (clicks < 100) {
            return tempMap.get("10-99");
        } else {
            return tempMap.get("100+");
        }
    }
}

