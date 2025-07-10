package com.cloudera.ps.example.fluentReader;

import java.io.InputStream;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

@Tags({ "fluent", "logs", "logfiles", "record", "reader" })
@CapabilityDescription("Reads fluentd msgpack records and outputs a Record representation")
public class FluentReader extends AbstractControllerService implements RecordReaderFactory {
    @Override
    public RecordReader createRecordReader(final Map<String, String> flowFile, final InputStream inputStream, long inputLength, final ComponentLog logger) {
        ParseFluent parser = new ParseFluent();
        parser.setLogger(logger);
        parser.setStream(inputStream);
        return new FluentRecordReader(logger, parser);
    }
}
