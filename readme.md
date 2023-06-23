# POC for Closable Stream Wrapping JDBC resources

## Notes
The Stream interface extends AutoClosable, usually this is meaningless BUT if the stream is backed by cloasable resources like JDBC resources, it needs to be used within a try with resource block.
JPA's 'getResultStream()' by default calls 'getResourceList.stream()' but Hibernate's implementation calls an internal 'stream()' method that NEEDS TO BE CLOSED.
