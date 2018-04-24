package com.shu.zyx.stream.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;


public class SaveDataBolt extends BaseBasicBolt {
    private static final Log LOG = LogFactory.getLog(SaveDataBolt.class);
    Map<String, Integer> counters = new HashMap<String, Integer>();

    public void prepare(Map stormConf, TopologyContext context) {

    }


    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        System.out.println(Thread.currentThread().getId() + "" + counters);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {


    }
}
