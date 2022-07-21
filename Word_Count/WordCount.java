//Date 06-sept-2021
//Reg. No - 20231

//importing the necessary libraries
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {  //the main class of the program

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{ //TokenizerMapper class , 4 parameteres , first 2 are inputs and last 2 are output type

    private final static IntWritable one = new IntWritable(1); //Intwrite as 1 , it will be the static value for each key
    private Text word = new Text(); //word will hold the key

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException { //map function contains the neccesary steps to be done in the mapping phase
      StringTokenizer itr = new StringTokenizer(value.toString()); //each line passed from input file is getting divided in the tokens
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken()); //key
        context.write(word, one); //passing <key , value>
      }
    }
  }

  public static class IntSumReducer //Reducer class
       extends Reducer<Text,IntWritable,Text,IntWritable> { //key is of type text and value is of type IntWritable
    private IntWritable result = new IntWritable(); //value to be passed by reducer

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException { //Reduce function
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get(); //sum of all the common occuring words stored in single specific key
      }
      result.set(sum);
      context.write(key, result); //passing out the result to output file 
    }
  }

  public static void main(String[] args) throws Exception { //main class
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count"); //job initialization
    job.setJarByClass(WordCount.class); //class name on which job is to be done
    job.setMapperClass(TokenizerMapper.class);//type of map class
    job.setCombinerClass(IntSumReducer.class);//type of combiner class
    job.setReducerClass(IntSumReducer.class); //type of reducer class
    job.setOutputKeyClass(Text.class); // output key type
    job.setOutputValueClass(IntWritable.class); // output value type
    FileInputFormat.addInputPath(job, new Path(args[0])); // path of the source file
    FileOutputFormat.setOutputPath(job, new Path(args[1]));// destination path , where the output will be kept
    System.exit(job.waitForCompletion(true) ? 0 : 1); // exit the job after completion
  }
}
