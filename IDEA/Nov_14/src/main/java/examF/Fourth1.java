package examF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Fourth1 {
    static long seq = 0;
    static String Guangzhou=null;
    static String Beijing=null;
    static String Shanghai=null;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ExamF");
        job.setJarByClass(Fourth1.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(Beijing);
        System.out.println(Shanghai);
        System.out.println(Guangzhou);
        System.exit(isok);
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            seq++;
            Text k3 = new Text();
            Text v3=new Text();
            String city=key.toString();
            String k3str=(seq+","+city+",");
            k3.set(k3str);

            long sumDingdan=0;//总订单数
            long sumPinglun=0;//总评论数
            double sumPingfen=0.0;//总评分
            long CountHotel=0;//酒店个数
            for (Text hotel:values) {
                String[] hoterStr=hotel.toString().split(",");
                sumDingdan+=Integer.parseInt(hoterStr[0]);
                sumPingfen+=Double.parseDouble(hoterStr[1]);
                sumPinglun+=Integer.parseInt(hoterStr[2]);
                CountHotel++;
            }
            double avgPingfen=sumPingfen/CountHotel;

            String v3str=(sumDingdan+","+avgPingfen+","+sumPinglun);
            if (city.equals("北京市")) Beijing = (city + "," + v3str);
            if (city.equals("广州市")) Guangzhou = (city + "," + v3str);
            if (city.equals("上海市")) Shanghai = (city + "," + v3str);
            v3.set(v3str);
            context.write(k3, v3);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text k2= new Text();
            Text v2 = new Text();
            String[] str = value.toString().split(",");
            if (str[11].length() == 0) str[11] = "0";
            if (str[12].length()==0) str[12] = "0";
            if (str[14].length()==0) str[14] = "0";
            if (str[4].length()!=0){
                k2.set(str[4]);//城市
//                            总订单            评分           点评数
                String CouStr = (str[14] + "," + str[11] + "," + str[12]);
                v2.set(CouStr);
                context.write(k2,v2);
            }

        }
    }
}