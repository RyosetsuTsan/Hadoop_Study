package examF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Second {
    static long Count=0;
    static long Deled=0;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ExamF");
        job.setJarByClass(Second.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        if (isok == 0) {
            long del  = Count-Deled;
            System.out.println("---删除的记录数为:" + del + "---");
        }
        System.exit(isok);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Count++;
            String[] str = value.toString().split(",");
            /*将字段{星级7、评论数12、评分11}中任意字段为空的数据删除，并打印输出删除条目数，*/
            if (str[7].length()!=0&&str[11].length()!=0&&str[12].length()!=0){
                Deled++;
                context.write(value, NullWritable.get());
            }
        }

    }
}