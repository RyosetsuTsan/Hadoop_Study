package examF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

public class Third {
    static long Count = 0;
    static long PingDel = 0;
    static long XingDel = 0;
    static long ChongDeled=0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ExamF");
        job.setJarByClass(Third.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        if (isok == 0) {
            System.out.println("---评分剔除的数据为:" +PingDel+"---");
            System.out.println("---星级剔除的数据为:" + XingDel + "---");
            System.out.println("---去重剔除的数据为:" + (Count-ChongDeled) + "---");
        }
        System.exit(isok);
    }

    public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            ChongDeled++;
            String[] str = key.toString().split(",");
            List<String> star = asList("一星","二星","三星","四星","五星");//创建arrays
            if (!star.contains(str[7])) {
                XingDel++;
            }
            if (Double.parseDouble(str[11])> 5) {
                PingDel++;
            }
            else if(star.contains(str[7])&&Double.parseDouble(str[11])< 5){
                context.write(key, NullWritable.get());
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str=value.toString().split(",");
            if (!str[11].equals("grade")) {
                Count++;//数量
                context.write(value, NullWritable.get());
            }
        }
    }
}