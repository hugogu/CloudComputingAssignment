import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job jobCounting = Job.getInstance(conf, "Link Count");
        FileSystem fs = FileSystem.get(conf);
        Path jobCountingOutput = new Path("/mp2/jobCounting");
        fs.delete(jobCountingOutput, true);
        
        jobCounting.setOutputKeyClass(IntWritable.class);
        jobCounting.setOutputValueClass(IntWritable.class);
        
        jobCounting.setMapOutputKeyClass(IntWritable.class);
        jobCounting.setMapOutputValueClass(IntWritable.class);
        
        jobCounting.setMapperClass(LinkCountMap.class);
        jobCounting.setReducerClass(LinkCountReduce.class);
        
        FileInputFormat.setInputPaths(jobCounting, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCounting, jobCountingOutput);
        
        jobCounting.setJarByClass(TopPopularLinks.class);
        jobCounting.waitForCompletion(true);
        
        Job topLinks = Job.getInstance(conf, "Top Links");
        topLinks.setOutputKeyClass(IntWritable.class);
        topLinks.setOutputValueClass(IntWritable.class);
        
        topLinks.setMapperClass(TopLinksMap.class);
        topLinks.setReducerClass(TopLinksReduce.class);
        
        topLinks.setMapOutputKeyClass(NullWritable.class);
        topLinks.setMapOutputValueClass(IntArrayWritable.class);
        
        FileInputFormat.setInputPaths(topLinks, jobCountingOutput);
        FileOutputFormat.setOutputPath(topLinks, new Path(args[1]));
        
        topLinks.setInputFormatClass(KeyValueTextInputFormat.class);
        topLinks.setOutputFormatClass(TextOutputFormat.class);
        
        topLinks.setJarByClass(TopPopularLinks.class);
        return topLinks.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        public static final Log LOG = LogFactory.getLog(LinkCountMap.class);
        private Map<Integer, Integer> linkCount = new HashMap<Integer, Integer>();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            LOG.info("LinkCountMap starting mapping.");
            final String[] parts = value.toString().split(":");
            final int id = Integer.parseInt(parts[0]);
            for(final String link : parts[1].split(" ")) {
                if (link == null || link.trim().equals(""))
                    continue;
                final int linkTo = Integer.parseInt(link.trim());
                addReference(linkTo);
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            LOG.info("LinkCountMap starting cleaning up.");
            for(final Entry<Integer, Integer> entry : linkCount.entrySet()) {
                context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
        
        private void addReference(int id) {
            Integer reference = linkCount.get(id);
            if (reference == null) {
                reference = 1;
            } else {
                reference++;
            }
            linkCount.put(id, reference);
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable id, Iterable<IntWritable> counts, Context context)  throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : counts) {
                sum += val.get();
            }
            context.write(id, new IntWritable(sum));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> topLinks = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            LOG.info("TopLinksMap starting mapping.");
            final Integer id = Integer.parseInt(key.toString());
            final Integer count = Integer.parseInt(value.toString());
            
            topLinks.add(new Pair<Integer, Integer>(count, id));
            
            if (topLinks.size() > this.N) {
                topLinks.remove(topLinks.first());
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            LOG.info("TopLinksMap starting cleaning up.");
            for(final Pair<Integer, Integer> entry : topLinks) {
                Integer[] values = { entry.first, entry.second };
                context.write(NullWritable.get(), new IntArrayWritable(values));
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> topLinks = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for(final IntArrayWritable value : values) {
                final IntWritable[] pair = (IntWritable[]) value.toArray();
                final Integer count = pair[0].get();
                final Integer id = pair[1].get();
                
                topLinks.add(new Pair<Integer, Integer>(count, id));
                if (topLinks.size() > this.N) {
                    topLinks.remove(topLinks.first());
                }
            }
            
            for(final Pair<Integer, Integer> entry : topLinks) {
                context.write(new IntWritable(entry.second), new IntWritable(entry.first));
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change