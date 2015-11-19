package brownfox;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.*;
import java.util.Map;
import java.util.HashMap;
import java.io.DataInput;
import java.io.DataOutput;

class WritableMap implements Writable{
  Map<Text,Writable> m;

  WritableMap(){
    m = new HashMap<Text,Writable>();
  }

  WritableMap(WritableMap m){
    this.m = new HashMap<Text,Writable>(m.m);
  }

  public <T extends Writable> T get(String key){
    return this.<T>get(new Text(key));
  }

  public <T extends Writable> T get(Text key){
    try{
        return (T)m.get(key);
    }catch(ClassCastException e){
        return null;
    }
  }

  public void put(Text key, Writable value){
    m.put(key,value);
  }

  public void put(String key, Writable value){
    m.put(new Text(key),value);
  }

  @Override
  public void readFields(DataInput in) throws IOException{
    int n = in.readInt();
    m = new HashMap<Text,Writable>();
    for(int i=0; i<n; i++){
      try{
        Text key = new Text();
        key.readFields(in);
        Class writableClass = Class.forName(in.readUTF());
        Writable value = (Writable) writableClass.newInstance();
        value.readFields(in);
        m.put(key,value);
      } catch (InstantiationException e) {
          e.printStackTrace();
      } catch (IllegalAccessException e) {
          e.printStackTrace();
      } catch (ClassNotFoundException e) {
          e.printStackTrace();
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException{
    out.writeInt(m.size());
    for( Map.Entry<Text,Writable> v : m.entrySet()){
      v.getKey().write(out);
      Writable value = v.getValue();
      out.writeUTF(value.getClass().getName());
      value.write(out);
    }
  }

  @Override
  public String toString(){
    String res = "";
    for (Map.Entry<Text,Writable> v : m.entrySet()){
      res += "_" + v.getValue();
    }
    return res;
  }
}
