package examF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Seventh_1 {
    static int top10 = 0;
    static String[] top = new String[10];

    public static class map extends Mapper<LongWritable, Text, RejWitable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] v1 = value.toString().split(",");
            //"省份,拒单率"
            RejWitable rejWitable = new RejWitable();
            rejWitable.setReject(Double.parseDouble(v1[1]));
            context.write(rejWitable, value);
        }
    }

    public static class reduce extends Reducer<RejWitable, Text, Text, NullWritable> {
        @Override
        protected void reduce(RejWitable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text vlaue : values) {
                if (top10< 10) {
                    top[top10] = vlaue.toString();
                    top10++;
                }
                context.write(vlaue, NullWritable.get());
            }
        }
    }

    public static class RejWitable implements WritableComparable<RejWitable> {
        private Double Reject;
        public Double getReject() {
            return Reject;
        }

        public void setReject(Double reject) {
            Reject = reject;
        }
        @Override
        public String toString() {
            return Reject+"";
        }


        public int compareTo(RejWitable other) {
            return this.Reject.compareTo(other.Reject)*-1;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(Reject);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.Reject= dataInput.readDouble();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "Examf");
        job.setJarByClass(Seventh_1.class);

        job.setMapperClass(map.class);
        job.setMapOutputKeyClass(RejWitable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        System.exit(isok);

    }
}
