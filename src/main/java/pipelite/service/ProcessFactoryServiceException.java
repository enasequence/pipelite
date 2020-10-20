package pipelite.service;

public class ProcessFactoryServiceException extends RuntimeException {
  public ProcessFactoryServiceException(String message) {
    super(message);
  }

  public ProcessFactoryServiceException(String message, Throwable cause) {
    super(message, cause);
  }
}
