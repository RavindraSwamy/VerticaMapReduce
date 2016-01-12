package com.abhi;

/**
 * Created by abhishek.srivastava on 1/11/16.
 */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
// Needed when using the setFromString method, which throws this exception.
import java.text.ParseException;

import org.apache.commons.collections.IterableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaInputFormat;
import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;
import org.codehaus.jackson.map.DeserializerFactory;

public class VerticaMapReduce extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, VerticaRecord, Text, LongWritable> {
        public static final Log log = LogFactory.getLog(Map.class);
        public void map(LongWritable key, VerticaRecord value, Context context) throws IOException, InterruptedException {
            log.info("+++++++++++++++++++++++ CAME INSIDE MAP +++++++++++++++++++++++++++++++++");
            System.out.println("+++++++++++++++++++++++ CAME INSIDE MAP +++++++++++++++++++++++++++++++++");
            if (value == null) {
                throw new IOException("vertica record is null");
            }
            try {
                if (value.get(1) != null && value.get(2) != null) {
                    System.out.println("value of name " + value.get(1));
                    System.out.println("value of val " + value.get(2));
                    context.write(new Text((String) value.get(1)), new LongWritable((Long) value.get(2)));
                }
            }
            catch(Exception ex) {
                log.error(ex.getMessage());
                log.error(ex.getStackTrace());
            }
            log.info("+++++++++++++++++++++++ COMPLETED MAP +++++++++++++++++++++++++++++++++");
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, VerticaRecord> {
        VerticaRecord record = null;
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try{
                record = new VerticaRecord(context.getConfiguration());
                System.out.println("++++++++ created record successfully++++++++++++++ ");
            } catch(Exception ex) {
                throw new IOException(ex);
            }
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            if (record == null) {
                throw new IOException("No output record found");
            }

            System.out.println("++++++++++++ key: " + key);
            LongWritable sum = new LongWritable(0);
            Iterator<LongWritable> it = values.iterator();
            while(it.hasNext()) {
                LongWritable t = it.next();
                System.out.println("++++++++++++ got value: " + t.get());
                sum.set(sum.get() + t.get());
            }

            try {
                record.set(0, key.toString());
                record.set(1, sum.get());
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("xxxxxx Error : " + ex.getMessage());
                System.out.println("xxxxxx Error : " + ex.getStackTrace());
            }

            context.write(new Text("Bar"), record);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        conf = job.getConfiguration();
        conf.set("mapreduce.job.tracker", "local");
        job.setJobName("vertica test");

        job.setInputFormatClass(VerticaInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VerticaRecord.class);
        job.setOutputFormatClass(VerticaOutputFormat.class);

        job.setJarByClass(VerticaMapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        VerticaOutputFormat.setOutput(job, "Bar", true, "name varchar", "total int");
        VerticaInputFormat.setInput(job, "select * from Foo where key > ?", "0");
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VerticaMapReduce(), args);
        System.exit(res);
    }
}
