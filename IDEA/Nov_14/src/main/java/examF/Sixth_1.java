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

public class Sixth_1 {
    static int ten = 0;
    static String[] tenth=new String[10];

    public static class map extends Mapper<LongWritable, Text, CityWitable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] v1 = value.toString().split(",");
            //"省份 城市，酒店数量，房间数"
            CityWitable cityWitable = new CityWitable();
            cityWitable.setNumroom(Integer.parseInt(v1[2]));
            context.write(cityWitable, value);
        }
    }

    public static class reduce extends Reducer<CityWitable, Text, Text, NullWritable> {
        @Override
        protected void reduce(CityWitable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text vlaue : values) {
                if (ten<10){
                    tenth[ten]=vlaue.toString();
                    ten++;
                }
                context.write(vlaue, NullWritable.get());
            }
        }
    }

    public static class CityWitable implements WritableComparable<CityWitable> {
        private Integer numroom;

        public Integer getNumroom() {
            return numroom;
        }

        public void setNumroom(Integer numroom) {
            this.numroom = numroom;
        }

        @Override
        public String toString() {
            return numroom+"";
        }

        public int compareTo(CityWitable other) {
            return this.numroom.compareTo(other.numroom) * -1;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(numroom);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.numroom = dataInput.readInt();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "Examf");
        job.setJarByClass(Sixth_1.class);
        job.setMapperClass(map.class);
        job.setMapOutputKeyClass(CityWitable.class);
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
