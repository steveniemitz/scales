namespace java com.tellapart.test

exception ExampleServiceException {
  1: required string message,
}

struct Message {
  /** The message content. */
  1: required string content
}

struct ServiceResponse {
  /** The response content. */
  1: required string content
}

service ExampleService {

  /** Pass a Message to this service and receive a response. */
  ServiceResponse passMessage(1: Message message)
      throws (1: ExampleServiceException serviceException),
}
