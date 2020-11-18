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

public class Sixth {
    public static class map extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text k2=new Text();
            Text v2=new Text();

            String[] str=value.toString().split(",");
            k2.set(str[3]+" "+str[4]);
            v2.set(Integer.parseInt(str[9])+"");
            context.write(k2,v2);
        }
    }
    public static class reduce extends Reducer <Text,Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long sumroom=0;
            long countcity=0;
            for (Text t:values) {
                sumroom+=Integer.parseInt(t.toString());
                countcity++;
            }
            String k=key.toString()+","+countcity+","+sumroom;
            context.write(new Text(k),NullWritable.get());

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con,"Examf");
        job.setJarByClass(Sixth.class);
        job.setMapperClass(map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        System.exit(isok);

    }
}
