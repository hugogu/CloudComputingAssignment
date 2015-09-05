import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PopularityLeague extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(PopularityLeague.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
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
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job jobCounting = Job.getInstance(conf, "Popularity League");
        FileSystem fs = FileSystem.get(conf);
        Path jobCountingOutput = new Path("/mp2/jobLeague");
        fs.delete(jobCountingOutput, true);
        
        jobCounting.setOutputKeyClass(IntWritable.class);
        jobCounting.setOutputValueClass(IntWritable.class);
        
        jobCounting.setMapOutputKeyClass(IntWritable.class);
        jobCounting.setMapOutputValueClass(IntWritable.class);
        
        jobCounting.setMapperClass(LinkCountMap.class);
        jobCounting.setReducerClass(LinkCountReduce.class);
        
        FileInputFormat.setInputPaths(jobCounting, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCounting, jobCountingOutput);
        
        jobCounting.setJarByClass(PopularityLeague.class);
        jobCounting.waitForCompletion(true);
        
        Job popRank = Job.getInstance(conf, "Top Links");
        popRank.setOutputKeyClass(IntWritable.class);
        popRank.setOutputValueClass(IntWritable.class);
        
        popRank.setMapperClass(PageRankMap.class);
        popRank.setReducerClass(PageRankReduce.class);
        
        popRank.setMapOutputKeyClass(NullWritable.class);
        popRank.setMapOutputValueClass(IntArrayWritable.class);
        
        FileInputFormat.setInputPaths(popRank, jobCountingOutput);
        FileOutputFormat.setOutputPath(popRank, new Path(args[1]));
        
        popRank.setInputFormatClass(KeyValueTextInputFormat.class);
        popRank.setOutputFormatClass(TextOutputFormat.class);
        
        popRank.setJarByClass(PopularityLeague.class);
        return popRank.waitForCompletion(true) ? 0 : 1;
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

    public static class PageRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private Set<Integer> league = new HashSet<Integer>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            for(final String leagueID : readHDFSFile(leaguePath, conf).split("\n")) {
                if (leagueID != null || !leagueID.equals("")) {
                    league.add(Integer.parseInt(leagueID));
                }
            }
        }
        
        /**
         * This is really doing a projection.
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            LOG.info("PageRankMap starting mapping.");
            final Integer id = Integer.parseInt(key.toString());
            final Integer count = Integer.parseInt(value.toString());
            
            if (league.contains(id)) {
                Integer[] values = { count, id };
                context.write(NullWritable.get(), new IntArrayWritable(values));
            }
        }
    }

    public static class PageRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {        
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            LOG.info("PageRankMap starting reducing.");
            List<Pair<Integer, Integer>> pages = new ArrayList<Pair<Integer, Integer>>();
            for(final IntArrayWritable value : values) {
                final IntWritable[] array = (IntWritable[]) value.toArray();
                final Integer count = array[0].get();
                final Integer id = array[1].get();
                pages.add(new Pair<Integer, Integer>(count, id));
            }
            
            for(final Pair<Integer, Integer> value : pages) {
                final Integer count = value.first;
                final Integer id = value.second;
                int rank = 0;
                for (Pair<Integer, Integer> ref : pages) {
                    if (ref.first < count)
                        rank++;
                }
                context.write(new IntWritable(id), new IntWritable(rank));
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