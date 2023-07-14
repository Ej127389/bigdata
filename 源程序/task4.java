import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.Vector;
public class task4 {
    public static class WebLog implements WritableComparable<WebLog> {
        private String remote_addr = "";
        private String remote_user = "";
        private String time_local = "";
        private String request = "";
        private String status = "";
        private String body_bytes_sent = "";
        private String http_referer = "";
        private String http_user_agent = "";

        public WebLog(){
            remote_addr = "";
            remote_user = "";
            time_local = "";
            request = "";
            status = "";
            body_bytes_sent = "";
            http_referer = "";
            http_user_agent = "";
        }

        public WebLog(WebLog c) {
            remote_addr = c.remote_addr;
            remote_user = c.remote_user;
            time_local = c.time_local;
            request = c.request;
            status = c.status;
            body_bytes_sent = c.body_bytes_sent;
            http_referer = c.http_referer;
            http_user_agent = c.http_user_agent;
        }

        public void setWebLog(String str){
            String temp = new String(str);
            int index = temp.indexOf(' ');
            remote_addr = temp.substring(0,index);
            temp = temp.substring(index + 1);
            index = temp.indexOf('[');
            remote_user = temp.substring(0,index-1);
            temp = temp.substring(index);
            index = temp.indexOf(']');
            time_local = temp.substring(0,index+1);
            temp = temp.substring(index+3);
            index = temp.indexOf('"');
            request = '"' + temp.substring(0,index+1);
            temp = temp.substring(index+2);
            index = temp.indexOf(' ');
            status = temp.substring(0,index);
            temp = temp.substring(index+1);
            index = temp.indexOf(' ');
            body_bytes_sent = temp.substring(0,index);
            temp = temp.substring(index+1);
            index = temp.indexOf(' ');
            http_referer = temp.substring(0,index);
            temp = temp.substring(index+1);
            http_user_agent = temp;
        }

        public String getTime_local(){
            return time_local;
        }

        @Override
        public int compareTo(WebLog o) {
            //return remote_addr.compareTo(o.remote_addr);
            return 1;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(remote_addr);
            dataOutput.writeUTF(remote_user);
            dataOutput.writeUTF(time_local);
            dataOutput.writeUTF(request);
            dataOutput.writeUTF(status);
            dataOutput.writeUTF(body_bytes_sent);
            dataOutput.writeUTF(http_referer);
            dataOutput.writeUTF(http_user_agent);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            remote_addr = dataInput.readUTF();
            remote_user = dataInput.readUTF();
            time_local = dataInput.readUTF();
            request = dataInput.readUTF();
            status = dataInput.readUTF();
            body_bytes_sent = dataInput.readUTF();
            http_referer = dataInput.readUTF();
            http_user_agent = dataInput.readUTF();
        }

        @Override
        public String toString(){
            return "remote_addr:" + remote_addr + "\n" +
                    "remote_user:" + remote_user + "\n" +
                    "time_local:" + time_local + "\n" +
                    "request:" + request  + "\n" +
                    "status:" + status + "\n" +
                    "body_bytes_sent:" + body_bytes_sent + "\n" +
                    "http_referer:" + http_referer + "\n" +
                    "http_user_agent:" + http_user_agent + "\n";
        }

    }

    public static class Task4Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            WebLog weblog = new WebLog();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                weblog.setWebLog(line);
                String time_ = weblog.getTime_local();
                int index = time_.indexOf(' ');
                time_ = time_.substring(1,index);// 18/Sep/2013:06:49:57
                String s1[] = time_.split("/");
                String s2[] = s1[2].split((":"));
                keyInfo.set(s2[0] + "/" + s1[1] + "/" + s1[0] + "/" + s2[1]);
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Task4Combine extends Reducer<Text, Text, Text, Text> {
        private Text valueInfo = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 统计
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            valueInfo.set(String.valueOf(sum));
            context.write(key, valueInfo);
        }
    }

    private static class Task4Reducer extends Reducer<Text, Text, Text, Text> {
        private Text valueInfo = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            valueInfo.set(String.valueOf(sum));
            context.write(key, valueInfo);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(task4.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Task4Map.class);
        job.setCombinerClass(Task4Combine.class);
        job.setReducerClass(Task4Reducer.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
