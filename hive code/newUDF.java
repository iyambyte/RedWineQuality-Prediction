package org.samples.hive.training1;




import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class newUDF extends UDF {

    public Integer evaluate(String word, String search_word){

        if(word == null || search_word == null) {
            return null;
        }

        int count = 0;

        Pattern pattern = Pattern.compile(search_word);
        Matcher matcher = pattern.matcher(word);

        while (matcher.find()){
            count += 1;
        }

        return count;
    }
   
}