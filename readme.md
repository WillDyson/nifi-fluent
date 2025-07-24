# Fluent NiFi Record Reader

NiFi Record Reader for the Fluent Forward Protocol spec.

Protocol spec can be found here:
https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1

Logstash implementation can be found here:
https://github.com/logstash-plugins/logstash-codec-fluent

May require the following JVM arguments[^1]:
```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

[^1]: https://github.com/msgpack/msgpack-java/blob/799e2d188b13b07704d1708d4e10283fe6dfdc8f/msgpack-core/src/main/java/org/msgpack/core/buffer/DirectBufferAccess.java#L287
