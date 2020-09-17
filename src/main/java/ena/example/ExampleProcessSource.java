package ena.example;

import pipelite.process.ProcessSource;

import java.util.ArrayList;
import java.util.List;

public class ExampleProcessSource implements ProcessSource {
  private static final int DEFAULT_PRIORITY = 9;
  private final List<String> ids =
      new ArrayList<String>() {
        {
          add("ID001");
          add("ID002");
          add("ID003");
          add("ID004");
          add("ID005");
          add("ID006");
          add("ID007");
          add("ID008");
          add("ID009");
          add("ID005");
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
