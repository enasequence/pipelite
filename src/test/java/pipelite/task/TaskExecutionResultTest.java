package pipelite.task;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TaskExecutionResultTest {

  @Test
  public void getAttributesAsJson() {
    TaskExecutionResult result = TaskExecutionResult.success();
    result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, "test");
    assertThat(result.attributesJson()).isEqualTo("{\n" + "  \"host\" : \"test\"\n" + "}");
  }
}
