package edu.uta.cse6331;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
    public short tag;
    public int index;
    public double value;

    Elem () {}

    Elem ( short t, int idx, double v ) {
        tag = t; 
		index = idx; 
		value = v;
    }
    
    Elem(Elem e){
    	tag = e.tag;
    	index = e.index;
    	value = e.value;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
    
    public String toString() { return tag + ","+index+","+value;}
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
   

    Pair () {}

    Pair ( int ival, int jval ) {
       i=ival; 
       j = jval; 
    }
    
    Pair(Pair p){
    	i = p.i;
    	j = p.j;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);

    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }
    
    public int compareTo(Pair p){
    	
    	if (this.i < p.i)
    		return -1;
    	else if (this.i == p.i)
    	{
    		if(this.j == p.j)
    			return 0;
    		if(this.j<p.j)
    			return -1;
    		return 1;
    	}
    	else 
    		return 1;   	
    }
    
    public String toString () { return i+","+j;}
    
}

public class Multiply {

	public static class MmatrixMapper extends Mapper<Object,Text,IntWritable,Elem > {
		short s = 0;
		@Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String vals[] = value.toString().split(",");
            Elem e = new Elem(s,Integer.parseInt(vals[0]),Double.parseDouble(vals[2]));
            context.write(new IntWritable(Integer.parseInt(vals[1])),e);
        }
    }

    public static class NmatrixMapper extends Mapper<Object,Text,IntWritable,Elem > {
    	short s = 1;
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String vals[] = value.toString().split(",");
            Elem e = new Elem(s,Integer.parseInt(vals[1]),Double.parseDouble(vals[2]));
            context.write(new IntWritable(Integer.parseInt(vals[0])),e);
        }
    }
    

	public static class FirstReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        static Vector<Elem> Melems = new Vector<Elem>();
		static Vector<Elem> Nelems = new Vector<Elem>();
		short t = 0;
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            Melems.clear();
            Nelems.clear();
			
            for (Elem v: values)
                if (v.tag == 0)
                    Melems.add(new Elem(v));
                else 
                	Nelems.add(new Elem(v));

            for ( Elem m: Melems )
                for ( Elem n: Nelems )
                    context.write(new Pair(m.index,n.index),new DoubleWritable(m.value*n.value)); 
        }
    }
	
	public static class SecondMapper extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable > {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
        	double v = value.get();
            context.write(new Pair(key),new DoubleWritable(v));
        }
    }
	
	public static class SecondReducer extends Reducer<Pair,DoubleWritable,Pair,Text> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;			
            for (DoubleWritable v: values)
				sum = sum + v.get();
            context.write(null,new Text(key.i+","+key.j+","+sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
		Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setReducerClass(FirstReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MmatrixMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,NmatrixMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]+"/temp"));
        job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]+"/temp"));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]+"/final"));
        job2.waitForCompletion(true);
		
    }
   
}
