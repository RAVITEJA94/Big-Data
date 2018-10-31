package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

class Vertex implements Writable {
	short tag; // 0 for a graph vertex, 1 for a group number
	long group; // the group where this vertex belongs to
	long VID; // the vertex ID
	ArrayList<Long> adjacent;

	Vertex(short t, long g, long id, ArrayList<Long> a) {
		this.tag = t;
		this.group = g;
		this.VID = id;
		this.adjacent = a;
	}

	Vertex(short t, long g) {
		this.tag = t;
		this.group = g;
		this.VID = 0;
		this.adjacent = new ArrayList<>();
	}

	Vertex() {
		this.tag = 0;
		this.group = 0;
		this.VID = 0;
		this.adjacent = new ArrayList<>();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		adjacent.clear();
		tag = arg0.readShort();
		group = arg0.readLong();
		VID = arg0.readLong();
		int size = arg0.readInt();
		for (int i = 0; i < size; i++) {
			Long item = arg0.readLong();
			adjacent.add(item);
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeShort(tag);
		arg0.writeLong(group);
		arg0.writeLong(VID);
		arg0.writeInt(adjacent.size());
		for (long item1 : adjacent) {
			arg0.writeLong(item1);
		}
	}

	public String toString() {
		return tag + " " + group + " " + VID + " " + adjacent.toString();
	}
}

public class Graph {

	public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
		short s = 0;

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			/* String val[] = value.toString().split(",");
 * 			 ArrayList<Long> val1= new ArrayList<>();
 * 			 			 for(int i=1;i<val.length;i++){
 * 			 			 			 val1.add(Long.parseLong(val[i]));
 * 			 			 			 			 }*/
			StringTokenizer st = new StringTokenizer(values.toString(), ",");
			int tc = st.countTokens();
			ArrayList<Long> val1 = new ArrayList<>();
			while (st.hasMoreElements()) {
				long vid = Long.parseLong(st.nextToken());
				for (int i = 1; i < tc; i++) {
					val1.add(Long.parseLong(st.nextToken()));
				}
				context.write(new LongWritable(vid), new Vertex(s, vid, vid, val1));
			}
		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
		short s = 1;

		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(value.VID), value);
			for (Long v : value.adjacent) {
				context.write(new LongWritable(v), new Vertex(s, value.group));
			}

		}
	}

	public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			Long m = Long.MAX_VALUE;
			short s = 0;
			ArrayList<Long> adj = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			
			for (Vertex v : values) {
				Vertex tempVertex = ReflectionUtils.newInstance(Vertex.class,conf);
				ReflectionUtils.copy(conf, v, tempVertex);
				if (tempVertex.tag == 0) {
					adj = tempVertex.adjacent;
				} // if loop
				m = Math.min(m, tempVertex.group);
			} // for loop
			context.write(new LongWritable(m), new Vertex(s, m, key.get(), adj));
		}
	}

	public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		long s = 1;

		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(key, new LongWritable(s));
		}

	}

	public static class FinalReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long m = 0;
			for (LongWritable v : values) {
				m = m + v.get();
			} // for loop
			context.write(key, new LongWritable(m));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("MyJob1");
		job1.setJarByClass(Graph.class);
		job1.setMapperClass(Mapper1.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Vertex.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Vertex.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
		job1.waitForCompletion(true);

		for (int i = 0; i < 5; i++) {
			Job job2 = Job.getInstance();
			job2.setJobName("MyJob2");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));
			job2.waitForCompletion(true);
		}
		Job job3 = Job.getInstance();
		job3.setJobName("MyJob3");
		job3.setJarByClass(Graph.class);

		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);
		job3.setMapperClass(FinalMapper.class);
		job3.setReducerClass(FinalReducer.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/final"));
		job3.waitForCompletion(true);
	}
}

