package examF;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class First {
    static long Count=0;
    static long Deled=0;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ExamF");
        job.setJarByClass(First.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        if (isok == 0) {
            long del  =Count-Deled;
            System.out.println("---删除的记录数为:" + del + "---");
        }
        System.exit(isok);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Count++;
            String[] str = value.toString().split(",");
            long count = 0;
            for (String s : str) {
                if (s.length() == 0)
                    count++;
            }
            if (count <= 3) {
                Deled++;
                context.write(value, NullWritable.get());
            }
        }
    }
}