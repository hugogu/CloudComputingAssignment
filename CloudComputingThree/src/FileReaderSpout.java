
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    final String fileName = (String)conf.get("datafile");
      try {
          final FileReader reader = new FileReader(fileName);
          final BufferedReader bufferedReader = new BufferedReader(reader);
          context.setTaskData("reader", bufferedReader);
      } catch (FileNotFoundException ex) {
      }
    
    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {
      final BufferedReader reader = (BufferedReader)this.context.getTaskData("reader");
      final String line;
      try {
          line = reader.readLine();
          if (line != null)
              _collector.emit(new Values(line));
      } catch (IOException ex) {
      }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void close() {
    final BufferedReader reader = (BufferedReader)this.context.getTaskData("reader");
      try {
          reader.close();
      } catch (IOException ex) {
      }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
