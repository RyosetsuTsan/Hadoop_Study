package MapRudece_sort;

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

public class JobMain {
    public static class Map extends Mapper<LongWritable, Text,PairWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //对每一行数据进行拆分，然后封装到Pairwritle对象中，作为K2
            String[]split=value.toString().split("\t");
            PairWritable pairWritable=new PairWritable();
            pairWritable.setFirst(split[0]);
            pairWritable.setSecond(Integer.parseInt(split[1]));
            //将k2 v2写入到上下文中
            context.write(pairWritable,value);
        }
    }
    public static class Reduce extends Reducer<PairWritable,Text,PairWritable, NullWritable> {
        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:values){
                context.write(key,NullWritable.get());
            }
        }
    }
    public static class PairWritable implements WritableComparable<PairWritable> {
        private String first;
        private int second;
        public String getFirst() {
            return first;
        }
        public void setFirst(String first) {
            this.first = first;
        }
        public int getSecond() {
            return second;
        }
        public void setSecond(int second) {
            this.second = second;
        }
        @Override
        public String toString() {
            return first + '\t' + second;
        }
        //实现排序规则
        public int compareTo(PairWritable pairWritle) {
            //先比较first，如果first相同，则比较second
            int result=this.first.compareTo(pairWritle.first);
            if (result==0){
                return  this.second-pairWritle.second;
            }
            return result;
        }//(升序)
        //实现序列化（固定操作
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(first);//字符串就UTF
            dataOutput.writeInt(second);//整型就INT

        }
        //实现反序列化
        public void readFields(DataInput dataInput) throws IOException {
            this.first=dataInput.readUTF();
            this.second=dataInput.readInt();
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();//获取配置对象信息
        // 创建一个任务对象
        Job job = Job.getInstance(conf, "mapreduce_sort");//
        job.setJarByClass(JobMain.class);//设置job运行主类
        //map
        job.setMapperClass(Map.class);//设置mapper类
        job.setMapOutputKeyClass(PairWritable.class);//设置map阶段k的输出类型
        job.setMapOutputValueClass(Text.class);//设置map阶段v的输出类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//文件来源
        //reduce
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交job，返回01
        int ret= job.waitForCompletion(true)? 0 : 1;
        System.exit(ret);
    }



}
