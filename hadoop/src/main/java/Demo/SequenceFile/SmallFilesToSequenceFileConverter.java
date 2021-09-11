package Demo.SequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
/**
 * @Description 通过SequenceFile合并小文件
 */
public class SmallFilesToSequenceFileConverter {

    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text fileNameKey;

        @Override
        protected void setup(Context context) {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            fileNameKey = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileNameKey, value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "SmallFilesToSequenceFileConverter");

        job.setJarByClass(SmallFilesToSequenceFileConverter.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SequenceFileMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        String outputPath = args[1];
        //FileUtils.deleteFileIfExists(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
