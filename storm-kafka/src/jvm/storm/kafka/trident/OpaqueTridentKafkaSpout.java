package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;


public class OpaqueTridentKafkaSpout implements IOpaquePartitionedTridentSpout<Map<String, List>, GlobalPartitionId, Map> {
    public static final Logger LOG = LoggerFactory.getLogger(OpaqueTridentKafkaSpout.class);
    
    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    
    public OpaqueTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }
    
    @Override
    public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext tc) {
        return new Coordinator(conf);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator<Map> {
        IBrokerReader reader;
        
        public Coordinator(Map conf) {
            reader = KafkaUtils.makeBrokerReader(conf, _config);
        }
        
        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }

        @Override
        public Map getPartitionsForBatch() {
            return reader.getCurrentBrokers();
        }
    }
    
    class Emitter implements IOpaquePartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;
        long _firstFetchTime   = 0;
        long _firstFetchOffset = 0;
        long _firstFetchCount  = 0;

        public Emitter(Map conf, TopologyContext context) {
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }

        @Override
        public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            try {
                SimpleConsumer consumer = _connections.register(partition);
                Map ret = KafkaUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
                _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long)ret.get("offset"));

                // if (LOG.isDebugEnabled()) {
                  long lastOffset = ((lastMeta == null) ? 0 : (Long)lastMeta.get("offset"));
                  long currOffset = ((ret      == null) ? 0 : (Long)ret.get("offset"));
                  long nextOffset = ((ret      == null) ? 0 : (Long)ret.get("nextOffset"));
                  long currCount  = ((lastMeta == null) ? 0 : (Long)ret.get("msgCount"));
                  long nextCount  = ((ret      == null) ? 0 : (Long)ret.get("nextCount"));
                  if (_firstFetchTime == 0) {
                      _firstFetchTime   = System.currentTimeMillis();
                      _firstFetchOffset = currOffset;
                      _firstFetchCount  = currCount;
                  }
                  
                  if ((currOffset != lastOffset) || (currOffset != nextOffset)) { // only when records this time or last
                      long currFetchTime = System.currentTimeMillis();
                      LOG.info(Utils.logString("KafkaSpout", ""+attempt, "batch", "", "#"+hashCode(),
                              "#",    String.format("%5d %8d %8d",        nextCount-currCount,   currCount, nextCount),
                              "rate", String.format("%5d %7d",            (1000 * (currCount - _firstFetchCount) / (1+currFetchTime - _firstFetchTime)), (1000 * (currOffset - _firstFetchOffset) / (1+currFetchTime - _firstFetchTime))),
                              "size", String.format("%8d %10d %10d %10d", nextOffset-currOffset, lastOffset, currOffset, nextOffset, (1000.0 * nextCount / (1+currFetchTime - _firstFetchTime)))
                              ));
                  }
                  //}
                return ret;
            } catch(FailedFetchException e) {
                LOG.warn("Failed to fetch from partition " + partition);
                if(lastMeta==null) {
                    return null;
                } else {
                    Map ret = new HashMap();
                    ret.put("offset", lastMeta.get("nextOffset"));
                    ret.put("nextOffset", lastMeta.get("nextOffset"));
                    ret.put("msgCount", lastMeta.get("msgCount"));
                    ret.put("newCount", lastMeta.get("msgCount"));
                    ret.put("partition", partition.partition);
                    ret.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
                    ret.put("topic", _config.topic);
                    ret.put("topology", ImmutableMap.of("name", _topologyName, "id", _topologyInstanceId));                    
                    
                    return ret;
                }
            }
        }

        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(Map<String, List> partitions) {
            return KafkaUtils.getOrderedPartitions(partitions);
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }    
}
