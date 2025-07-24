package com.cloudera.ps.example.fluentReader;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FluentRecordSchema extends SimpleRecordSchema {
    public static final RecordField TAG = new RecordField("tag", RecordFieldType.STRING.getDataType());
    public static final RecordField EPOCH = new RecordField("epoch", RecordFieldType.LONG.getDataType());
    public static final RecordField ENTRIES = new RecordField("entries", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));

    private static final List<RecordField> RECORD_FIELDS = Collections.unmodifiableList(Arrays.asList(
        TAG,
        EPOCH,
        ENTRIES
    ));

    public FluentRecordSchema() {
        super(RECORD_FIELDS);
    }
}