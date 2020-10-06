package pipeline.ena.example;

import pipelite.process.ProcessSource;

import java.util.ArrayList;
import java.util.List;

public class ExampleProcessSource implements ProcessSource {
  private static final int DEFAULT_PRIORITY = 9;
  private final List<String> ids =
      new ArrayList<String>() {
        {
          add("ID1");
          add("ID2");
          add("ID3");
          add("ID4");
          add("ID5");
          add("ID6");
          add("ID7");
          add("ID8");
          add("ID9");
        }
      };
  private int current = 0;

  @Override
  public NewProcess next() {
    if (current < ids.size()) {
      return new NewProcess(ids.get(current++), DEFAULT_PRIORITY);
    } else {
      return null;
    }
  }

  @Override
  public void accept(String processId) {}

  @Override
  public void reject(String processId) {}
}
