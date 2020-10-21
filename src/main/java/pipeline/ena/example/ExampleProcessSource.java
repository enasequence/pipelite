package pipeline.ena.example;

import org.springframework.stereotype.Component;
import pipelite.process.ProcessSource;

import java.util.Arrays;
import java.util.List;

@Component
public class ExampleProcessSource implements ProcessSource {

  private static final String PIPELINE_NAME = "example";
  private static final int PRIORITY = 9;

  private final List<String> ids =
      Arrays.asList("ID1", "ID2", "ID3", "ID4", "ID5", "ID6", "ID7", "ID8", "ID9");
  private int current = 0;

  @Override
  public String getPipelineName() {
    return PIPELINE_NAME;
  }

  @Override
  public NewProcess next() {
    if (current < ids.size()) {
      return new NewProcess(ids.get(current++), PRIORITY);
    } else {
      return null;
    }
  }

  @Override
  public void accept(String processId) {}

  @Override
  public void reject(String processId) {}
}
