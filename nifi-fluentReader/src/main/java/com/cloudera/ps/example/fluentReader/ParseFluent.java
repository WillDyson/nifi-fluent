package com.cloudera.ps.example.fluentReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.ArrayList;

import org.apache.nifi.logging.ComponentLog;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

public class ParseFluent {
    private ComponentLog logger;
    private InputStream dataInputStream;
    private MessageUnpacker unpacker;
    private ArrayList<FluentRecord> records;

    void setLogger(ComponentLog logger) {
        this.logger = logger;
    }

    void setStream(InputStream dataInputStream) {
        this.dataInputStream = dataInputStream;
    }

    void parse() throws IOException {
        if (dataInputStream == null) {
            throw new IOException("No stream attached to parser");
        }

        parseStream(this.dataInputStream);

        if (logger != null) {
            logger.info("Parsed %d messages", records.size());
        }
    }

    private void parseStream(InputStream dataInputStream) throws IOException {
        unpacker = MessagePack.newDefaultUnpacker(dataInputStream);
        records = new ArrayList<>();

        while(unpacker.hasNext()) {
            parseRecordSet(unpacker, records);
        }
    }

    void parseRecordSet(MessageUnpacker unpacker, ArrayList<FluentRecord> records)  throws IOException{
        ImmutableValue rawMessage = unpacker.unpackValue();
        if (!rawMessage.isArrayValue()) {
            throw new IOException("Top-level record not an array");
        }

        ArrayValue data = rawMessage.asArrayValue();

        if (data.size() < 2) {
            throw new IOException("Top-level array too small");
        }

        StringValue tag = data.get(0).asStringValue();
        Value entries = data.get(1);

        switch(entries.getValueType()) {
            case STRING:
                // reject gzipped logs
                if (data.size() >= 3 && data.get(2).isMapValue()) {
                    Map<Value, Value> option = data.get(2).asMapValue().map();

                    Value compression;
                    if (option != null && (compression = option.get(ValueFactory.newString("compressed"))) != null && compression.toString().equals("gzip")) {
                        throw new IOException("Compressed records not supported");
                    }
                }

                parseStringRecordSet(entries.asStringValue(), tag, records);
                break;

            case ARRAY:
                parseArrayRecordSet(entries.asArrayValue(), tag, records);
                break;

            case INTEGER:
                Value ts = entries;
                MapValue map = data.get(2).asMapValue();
                records.add(createRecord(map, ts, tag));
                break;

            default:
                throw new IOException("Unexpected entries type");
        }
    }

    private void parseStringRecordSet(StringValue entriesPayload, StringValue tag, ArrayList<FluentRecord> records) throws IOException {
        InputStream innerInputStream = new ByteArrayInputStream(entriesPayload.asStringValue().asByteArray());
        MessageUnpacker innerUnpacker = MessagePack.newDefaultUnpacker(innerInputStream);
        while(innerUnpacker.hasNext()) {
            ArrayValue encodedEntries = innerUnpacker.unpackValue().asArrayValue();
            Value ts = encodedEntries.get(0);
            MapValue map = encodedEntries.get(1).asMapValue();
            records.add(createRecord(map, ts, tag));
        }
    }

    private void parseArrayRecordSet(ArrayValue entries, StringValue tag, ArrayList<FluentRecord> records) throws IOException {
        ArrayValue innerEntries = entries.asArrayValue();
        for (int i = 0; i < innerEntries.size(); i++) {
            Value ts = innerEntries.get(i).asArrayValue().get(0);
            MapValue map = innerEntries.get(i).asArrayValue().get(1).asMapValue();
            records.add(createRecord(map, ts, tag));
        }
    }

    private FluentRecord createRecord(MapValue map, Value ts, StringValue tag) throws IOException {
        FluentRecord record = new FluentRecord();

        HashMap<String, String> entries = new HashMap<>();

        map.map().forEach((k, v) -> {
            entries.put(k.toString(), v.toString());
        });

        record.epoch = parseTimestamp(ts);
        record.entries = entries;
        record.tag = tag.toString();

        return record;
    }

    private long parseTimestamp(Value ts) {
        switch (ts.getValueType()) {
            case EXTENSION:
                ExtensionValue ext = ts.asExtensionValue();
                if (ext.getType() == 0) {
                    ByteBuffer buf = ByteBuffer.wrap(ts.asExtensionValue().getData());
                    return Integer.toUnsignedLong(buf.getInt());
                }
                return 0;

            case INTEGER:
                return ts.asIntegerValue().asLong();

            default:
                return 0;
        }
    }

    ArrayList<FluentRecord> getRecords() {
        return records;
    }

}
