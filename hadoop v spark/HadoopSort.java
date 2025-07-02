package com.example.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.log4j.Logger;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.util.Arrays;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Writable;

public class HadoopSort {

    public static class FixedInputFormat extends InputFormat<NullWritable, NullWritable>{

        // Define the FixedInputSplit class (no need for 'static' if it's not an inner class)
        public static class FixedInputSplit extends InputSplit implements Writable {
            @Override
            public long getLength() throws IOException {
                return 1;  // Arbitrary
            }

            @Override
            public String[] getLocations() throws IOException {
                return new String[0];  // No locality
            }

             @Override
            public void write(DataOutput out) throws IOException {
                out.writeInt(6);
                out.writeInt(9);
            }

            @Override
            public void readFields(DataInput in) throws IOException {
                    // DO NOTHING 
            }
        }

        // Implement the getSplits method to create a fixed number of input splits
        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException {
            int numSplits = context.getConfiguration().getInt("num.map.tasks", 8);  // Default to 32 mappers
            List<InputSplit> splits = new ArrayList<>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                splits.add(new FixedInputSplit());
            }
            return splits;
        }

        // Implement the createRecordReader method
        @Override
        public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new RecordReader<NullWritable, NullWritable>() {
                private boolean done = false;

                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) {
                    done = false;
                }

                @Override
                public boolean nextKeyValue() {
                    if (!done) {
                        done = true;
                        return true;
                    }
                    return false;
                }

                @Override
                public NullWritable getCurrentKey() {
                    return NullWritable.get();
                }

                @Override
                public NullWritable getCurrentValue() {
                    return NullWritable.get();
                }

                @Override
                public float getProgress() {
                    return done ? 1.0f : 0.0f;
                }

                @Override
                public void close() {
                }
            };
        }
    }


    public static class HashRecord implements WritableComparable<HashRecord> {
        private byte[] nonce;
        private byte[] hash;

        public HashRecord() {
            this.hash = new byte[10];
            this.nonce = new byte[6];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(hash);
            out.write(nonce);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            in.readFully(hash);
            in.readFully(nonce);
        }

        @Override
        public int compareTo(HashRecord other) {
            // Assuming the hash array is the primary sort key
            for (int i = 0; i < this.hash.length; i++) {
                int thisVal = this.hash[i] & 0xFF;
                int otherVal = other.hash[i] & 0xFF;
                if (thisVal != otherVal) {
                    return thisVal - otherVal;
                }
            }
            // If hashes are identical, you could compare nonces or just consider them equal
            return 0;
        }
    }

    public static class BinaryOutputFormat extends FileOutputFormat<HashRecord, NullWritable> {
        @Override
        public RecordWriter<HashRecord, NullWritable> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException {
            Path file = getDefaultWorkFile(job, "");
            FileSystem fs = file.getFileSystem(job.getConfiguration());
            FSDataOutputStream fileOut = fs.create(file, false);
            return new BinaryRecordWriter(fileOut);
        }

        protected static class BinaryRecordWriter extends RecordWriter<HashRecord, NullWritable> {
            private DataOutputStream out;

            public BinaryRecordWriter(DataOutputStream out) {
                this.out = out;
            }

            @Override
            public void write(HashRecord key, NullWritable value) throws IOException, InterruptedException {
                out.write(key.hash);
                out.write(key.nonce);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                out.close();
            }
        }
    }


    public static class HashMapper extends Mapper<NullWritable, NullWritable, HashRecord, NullWritable> {
        private Random random = new Random();
        private long numRecords;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numRecords = Integer.parseInt(context.getConfiguration().get("numRecords"));
        }

        @Override
        protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
		Blake3 hasher = Blake3.newInstance();
		byte[] nonce = new byte[6];
		HashRecord record = new HashRecord();
            	for (long i = 0; i < numRecords; i++) {
                	//byte[] nonce = new byte[6];
                	random.nextBytes(nonce);

                	//Blake3 hasher = Blake3.newInstance();
                	hasher.update(nonce);
                	byte[] hash = hasher.digest(10);  // Produce a 10-byte hash

                	//HashRecord record = new HashRecord();
                	System.arraycopy(hash, 0, record.hash, 0, hash.length);
                	System.arraycopy(nonce, 0, record.nonce, 0, nonce.length);

                	context.write(record, NullWritable.get());
            	}
        	}
    	}

    public static class HashReducer extends Reducer<HashRecord, NullWritable, HashRecord, NullWritable> {
        //private static final Logger LOG = Logger.getLogger(HashMapper.class);

        @Override
        protected void reduce(HashRecord key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //LOG.info("Reducer writing record");

            for (NullWritable val : values) {
                 context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //if (args.length != 4) {
        //    System.err.println("Usage: HadoopSort <input path> <output path> <num records per mapper>");
        //    System.exit(-1);
        //}

        Configuration conf = new Configuration();
        conf.set("numRecords", args[2]);
        //conf.set("mapreduce.map.output.compress", "true");
        //conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        //conf.setInt("mapreduce.job.maps", Integer.parseInt(args[3]));
        //Set the heap size for the JVM that runs the mapper.
        //conf.set("mapreduce.map.java.opts", "-Xmx1024m");
        //conf.set("mapreduce.map.java.opts", "-Xmx32m");

        // Set the amount of memory allocated to each map task.
        //conf.set("mapreduce.map.memory.mb", "2048");
        //conf.set("mapreduce.map.memory.mb", "2048");
        // Set the spill percentage which controls the memory usage, after which a spill to disk occurs.
        //conf.set("mapreduce.task.io.sort.mb", "512");
        //conf.set("mapreduce.task.io.sort.mb", "64");
        //conf.set("mapreduce.map.sort.spill.percent", "0.90");

        Job job = Job.getInstance(conf, "BLAKE3 Hash Generator");
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        job.setJarByClass(HadoopSort.class);
        job.setMapperClass(HashMapper.class);
        job.setReducerClass(HashReducer.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(FixedInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(HashRecord.class);
        job.setOutputValueClass(NullWritable.class);
        conf.setInt("mapreduce.job.maps", Integer.parseInt(args[3]));
        job.setNumReduceTasks(1); // Set number of reducers
        job.setOutputFormatClass(BinaryOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, false);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}