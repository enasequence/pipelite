package pipelite.stage.parameters;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AwsBatchExecutorParametersTest {

  @Test
  public void validate() {
    assertThrows(PipeliteException.class, () -> new AwsBatchExecutorParameters().validate());
    assertThrows(
        PipeliteException.class, () -> AwsBatchExecutorParameters.builder().build().validate());
    AwsBatchExecutorParameters.builder().definition("test").queue("test").build().validate();
  }
}
