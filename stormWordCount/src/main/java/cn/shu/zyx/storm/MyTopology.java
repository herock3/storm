package cn.shu.zyx.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class MyTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("mySpout",new MySpout(),2);
        builder.setBolt("splitBolt",new MySplitBolt(),5).shuffleGrouping("mySpout");
        builder.setBolt("countBolt",new MyCountBolt(),5).fieldsGrouping("splitBolt",new Fields("word"));
        Config config=new Config();
        config.setNumWorkers(2);

        StormSubmitter.submitTopology("myWordCount",config,builder.createTopology());
//        LocalCluster local=new LocalCluster();
//        local.submitTopology("myWordCount",config,builder.createTopology());
    }

}
