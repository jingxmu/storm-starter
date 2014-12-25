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

		// key:fields value:fields�����Ե�streamID
		_fieldLocations = new HashMap<String, GlobalStreamId>();
		_collector = collector;
		int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS))
				.intValue();
		_pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(
				timeout, new ExpireCallback());

		// context.getThisSources()�õ��������bolt��Դcomponent��<streamId,
		// ���stream��grouping��ʽ>
		_numSources = context.getThisSources().size();
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(
					source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (idFields == null)
				idFields = setFields;
			else
				idFields.retainAll(setFields); // �õ����source
												// fields�Ľ�����Ҳ���ǿ���join��fields

			// ���ڰ���output fields��source�����м�¼
			for (String outfield : _outFields) {
				for (String sourcefield : fields) {
					if (outfield.equals(sourcefield)) {
						_fieldLocations.put(outfield, source);// ����Ҫ��output
																// fields���������ĸ�source
																// stream��
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
		List<Object> id = tuple.select(_idFields); // �������tuple��idFields��Ӧ��ֵ��list
		GlobalStreamId streamId = new GlobalStreamId(
				tuple.getSourceComponent(), tuple.getSourceStreamId());
		if (!_pending.containsKey(id)) {
			_pending.put(id, new HashMap<GlobalStreamId, Tuple>());
		}

		// �õ����join fields����Ӧ������stream��tuple
		Map<GlobalStreamId, Tuple> parts = _pending.get(id);
		if (parts.containsKey(streamId))
			throw new RuntimeException(
					"Received same side of single join twice");
		parts.put(streamId, tuple);
		if (parts.size() == _numSources) { // ������ͬ��stream�У�����ͬ�� join fields
											// valueֻ��һ��tuple���������յĽ��ֻ��������stream�г��ֵ�join
											// fields�������
			_pending.remove(id);
			List<Object> joinResult = new ArrayList<Object>();
			String joinRst = "";
			for (String outField : _outFields) {
				GlobalStreamId loc = _fieldLocations.get(outField);
				joinResult.add(parts.get(loc).getValueByField(outField));
				joinRst = joinRst + parts.get(loc).getValueByField(outField)+"\t";
			}

			// һ��output tuple�ɶ��source tuple���������
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
