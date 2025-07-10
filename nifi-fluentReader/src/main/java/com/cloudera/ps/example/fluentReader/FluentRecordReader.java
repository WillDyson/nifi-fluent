package com.cloudera.ps.example.fluentReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordSchema;

public class FluentRecordReader implements RecordReader { private final ComponentLog logger;
    private final ParseFluent parser;
    private Iterator<FluentRecord> recordIter;

    public FluentRecordReader(final ComponentLog logger, ParseFluent parser) {
        this.logger = logger;
        this.parser = parser;
    }

    @Override
    public void close() { }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException {
        if (recordIter == null) {
            try {
                this.parser.parse();
                this.recordIter = this.parser.getRecords().iterator();
            } catch (IOException e) {
                logger.error("Ran past stream", e);
            }
        }

        if (this.recordIter.hasNext()) {
            return createRecord(this.recordIter.next());
        }

        return null;
    }

    private Record createRecord(FluentRecord fluentRecord) {
        return new MapRecord(getSchema(), Collections.unmodifiableMap(fluentRecord.toMap()));
    }

    @Override
    public RecordSchema getSchema() {
        RecordField tagField = new RecordField("tag", RecordFieldType.STRING.getDataType());
        RecordField epochField = new RecordField("epoch", RecordFieldType.LONG.getDataType());
        RecordField entriesField = new RecordField("entries", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));

        return new SimpleRecordSchema(List.of(tagField, epochField, entriesField));
    }
}