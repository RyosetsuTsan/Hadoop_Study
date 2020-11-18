package Tsam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowCount {
    static LongWritable lw=new LongWritable();
    static Text txt=new Text();
    public static class map extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分手机号
            String[] s = value.toString().split("\t");
            String phone=s[1];
            //获取四个流量字段
            FlowBean fb=new FlowBean();
            fb.setUpFlow(Integer.parseInt(s[6]));
            fb.setDownFlow(Integer.parseInt(s[7]));
            fb.setUpCountFlow(Integer.parseInt(s[8]));
            fb.setDownCountFlow(Integer.parseInt(s[9]));

            txt.set(phone);
            context.write(txt,fb);
        }
    }

    public static class reduce extends Reducer<Text,FlowBean,Text,FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            //k3不变
            //v3遍历集合相加 
            FlowBean flowBean = new FlowBean();
            Integer upFlow=0;
            Integer downFlow=0;
            Integer upCountFlow=0;
            Integer downCountFlow=0;
            for (FlowBean value:values) {
                upFlow+=value.getUpFlow();
                downFlow+=value.getDownFlow();
                upCountFlow+=value.getUpCountFlow();
                downCountFlow+=value.getDownCountFlow();
            }
            flowBean.setUpFlow(upFlow);
            flowBean.setDownFlow(downFlow);
            flowBean.setUpCountFlow(upCountFlow);
            flowBean.setDownCountFlow(downCountFlow);

            context.write(key,flowBean);
        }
    }

    public static class FlowBean implements Writable {
        private Integer upFlow;
        private Integer downFlow;
        private Integer upCountFlow;
        private Integer downCountFlow;

        public Integer getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(Integer upFlow) {
            this.upFlow = upFlow;
        }

        public Integer getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(Integer downFlow) {
            this.downFlow = downFlow;
        }

        public Integer getUpCountFlow() {
            return upCountFlow;
        }

        public void setUpCountFlow(Integer upCountFlow) {
            this.upCountFlow = upCountFlow;
        }

        public Integer getDownCountFlow() {
            return downCountFlow;
        }

        public void setDownCountFlow(Integer downCountFlow) {
            this.downCountFlow = downCountFlow;
        }

        @Override
        public String toString() {
            return upFlow +
                    "\t" + downFlow +
                    "\t" + upCountFlow +
                    "\t" + downCountFlow;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(upFlow);
            dataOutput.writeInt(downFlow);
            dataOutput.writeInt(upCountFlow);
            dataOutput.writeInt(downCountFlow);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.upFlow=dataInput.readInt();
            this.downFlow=dataInput.readInt();
            this.upCountFlow=dataInput.readInt();
            this.downCountFlow=dataInput.readInt();

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FlowCount");
        job.setJarByClass(FlowCount.class);

        //map
        job.setMapperClass(map.class);//设置mapper类
        job.setMapOutputKeyClass(Text.class);//设置map阶段k的输出类型
        job.setMapOutputValueClass(FlowBean.class);//设置map阶段v的输出类型
        FileInputFormat.setInputPaths(job,new Path(args[0]));//文件来源

        //reduce
        job.setReducerClass(reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交job，返回01
        int ret= job.waitForCompletion(true)? 0 : 1;
        System.exit(ret);
    }

}
