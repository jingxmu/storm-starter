package storm.starter.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

public class SingleJoinBolt extends BaseRichBolt {
	OutputCollector _collector;
	Fields _idFields;
	Fields _outFields;
	int _numSources;
	TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
	Map<String, GlobalStreamId> _fieldLocations;
	HashSet<String> results = new HashSet<String>();

	public SingleJoinBolt(Fields outFields) {
		_outFields = outFields;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		// key:fields value:fields所来自的streamID
		_fieldLocations = new HashMap<String, GlobalStreamId>();
		_collector = collector;
		int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS))
				.intValue();
		_pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(
				timeout, new ExpireCallback());

		// context.getThisSources()得到的是这个bolt的源component的<streamId,
		// 这个stream的grouping方式>
		_numSources = context.getThisSources().size();
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(
					source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (idFields == null)
				idFields = setFields;
			else
				idFields.retainAll(setFields); // 得到多个source
												// fields的交集，也就是可以join的fields

			// 对于包含output fields的source都进行记录
			for (String outfield : _outFields) {
				for (String sourcefield : fields) {
					if (outfield.equals(sourcefield)) {
						_fieldLocations.put(outfield, source);// 所需要的output
																// fields是来自于哪个source
																// stream的
					}
				}
			}
		}
		_idFields = new Fields(new ArrayList<String>(idFields));

		if (_fieldLocations.size() != _outFields.size()) {
			throw new RuntimeException(
					"Cannot find all outfields among sources");
		}
	}

	@Override
	public void execute(Tuple tuple) {
		List<Object> id = tuple.select(_idFields); // 返回这个tuple的idFields对应的值的list
		GlobalStreamId streamId = new GlobalStreamId(
				tuple.getSourceComponent(), tuple.getSourceStreamId());
		if (!_pending.containsKey(id)) {
			_pending.put(id, new HashMap<GlobalStreamId, Tuple>());
		}

		// 得到这个join fields所对应的所有stream和tuple
		Map<GlobalStreamId, Tuple> parts = _pending.get(id);
		if (parts.containsKey(streamId))
			throw new RuntimeException(
					"Received same side of single join twice");
		parts.put(streamId, tuple);
		if (parts.size() == _numSources) { // 由于相同的stream中，对相同的 join fields
											// value只有一个tuple，而且最终的结果只对在所有stream中出现的join
											// fields产生输出
			_pending.remove(id);
			List<Object> joinResult = new ArrayList<Object>();
			String joinRst = "";
			for (String outField : _outFields) {
				GlobalStreamId loc = _fieldLocations.get(outField);
				joinResult.add(parts.get(loc).getValueByField(outField));
				joinRst = joinRst + parts.get(loc).getValueByField(outField)+"\t";
			}

			// 一个output tuple由多个source tuple产生的情况
			_collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);
			results.add(joinRst);

			for (Tuple part : parts.values()) {
				_collector.ack(part);
			}
		}
	}

	@Override
	public void cleanup() {
		System.out.println("=====================");
		for (String rst : results) {
			System.out.println(rst);
		}
		System.out.println("=====================");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_outFields);
	}

	private class ExpireCallback
			implements
			TimeCacheMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
		@Override
		public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
			for (Tuple tuple : tuples.values()) {
				_collector.fail(tuple);
			}
		}
	}
}
