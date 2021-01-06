package pipelite.stage.parameters;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LsfExecutorParametersTest {

  @Test
  public void validate() {
    assertThrows(PipeliteException.class, () -> new LsfExecutorParameters().validate());
    assertThrows(PipeliteException.class, () -> LsfExecutorParameters.builder().build().validate());
    LsfExecutorParameters.builder()
        .definition("pipelite/executor/lsf.yaml")
        .format(LsfExecutorParameters.Format.YAML)
        .build()
        .validate();
  }
}
