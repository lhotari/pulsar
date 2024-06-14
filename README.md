# Reproducer for grpc-netty, vertx-netty and jetcd issue with Netty 4.1.111.Final

Issue reported as https://github.com/grpc/grpc-java/issues/11284

### Reproducing


Clone the reproducer branch

```shell
git clone -b lh-grpc-netty-4.1.111 --depth 20 https://github.com/lhotari/pulsar
cd pulsar
```

Build pulsar-metadata module

```shell
mvn -pl pulsar-metadata -am install -DskipTests -Dspotbugs.skip=true -Dcheckstyle.skip=true 
```

Reproduce exception `io.grpc.StatusRuntimeException: INTERNAL: Encountered end-of-stream mid-frame`

```shell
mvn -pl pulsar-metadata -DredirectTestOutputToFile=false -DtestRetryCount=0 test -Dtest=MetadataStoreTest#getChildrenTest
```

Reproduce hang

```shell
mvn -pl pulsar-metadata -DredirectTestOutputToFile=false -DtestRetryCount=0 test -Dtest=MetadataStoreTest#testConcurrentPutGetOneKey
```

You can pass `-Dnetty.version=4.1.110.Final` on the mvn command line to see the test pass with Netty 4.1.110.Final.