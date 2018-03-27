package com.zhouze;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;


/**
 * Hadoop Demo程序：
 * 可以单机运行，通过Debug观察Map的in,out 值情况以及处理过程。
 *
 * 备注：
 * 根据日志文件格式进行字符串解析，获取指定的字段信息组成Reduce key, 统计上述key的出现次数。
 */
public class WordCount {

    
    /**
     * Mapper 实现类：
     * @author zhouze
     *
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word;
        private String imei;
        private String areacode;
        private String responsedata;
        private String requesttime;
        private String requestip;

        
        /**
         * map阶段的key-value格式是由输入格式所决定的， 
         * 如果默认是TextInputFormat, 则每行作为一个记录进程处理，其中Key作为此行的开头相对于文件的起始位置.
         * 注意： Map阶段输出的Key,
         * Value必须与Reduce阶段输入的Key,value 类型一致。
         * 
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // super.map(key, value, context);

            String jsonStr = value.toString().substring(20);
            Map<String, String> map = this.jsonToMap(jsonStr);

            word = new Text();
            imei = this.jsonToMap(map.get("requestinfo")).get("imei");
            areacode = map.get("areacode");
            responsedata = map.get("responsedata");
            requesttime = map.get("requesttime");
            requestip = map.get("requestip");

            // 备注：
            // 此处设定one 恒= 1.
            // word: Map_out_key , Reduce_in_key
            // one: Map_out_value , Reduce_in_value
            String wd = imei + "\t" + areacode + "\t" + responsedata + "\t" + requesttime + "\t" + requestip;
            word.set(wd);
            context.write(word, one);

        }


        private Map<String, String> jsonToMap(String jsonStr) {
            return JSON.parseObject(jsonStr, new TypeReference<Map<String, String>>(){});
        }

    }

    
    /**
     * Reducer 实现类：
     * @author zhouze
     *
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        /**
         * Reducer函数：
         * 
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    
    /**
     * main func:
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input_file_path = "/Users/zhouze/Documents/AppWorkspace/IdeaProjects/HadoopWordCount/src/main/resources/input";
        String output_file_path = "/Users/zhouze/Documents/AppWorkspace/IdeaProjects/HadoopWordCount/src/main/resources/output";
        String[] otherArgs = new String[] { input_file_path, output_file_path };

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        // Job job = new Job(conf, "word count");
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
