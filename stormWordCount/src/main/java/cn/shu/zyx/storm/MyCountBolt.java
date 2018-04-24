package cn.shu.zyx.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


public class MyCountBolt extends BaseRichBolt {
    OutputCollector collector;
    int count;
    Map<String,Integer> map=new HashMap <String, Integer>();
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple input) {
        String word=input.getString(0);
        int number=input.getInteger(1);
//        System.out.println(Thread.currentThread().getId()+ " word:"+word);
        if(map.containsKey(word)){
           int count=map.get(word);
           map.put(word,count+number);
        }else {
            map.put(word,1);
        }
        System.out.println("count:"+map);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
