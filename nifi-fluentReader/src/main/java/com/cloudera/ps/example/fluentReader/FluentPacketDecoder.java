package com.cloudera.ps.example.fluentReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.MapRecord;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@ChannelHandler.Sharable
public class FluentPacketDecoder extends MessageToMessageDecoder<ByteBuf> {
    private final FluentRecordSchema FLUENT_RECORD_SCHEMA = new FluentRecordSchema();

    private final ComponentLog logger;

    public FluentPacketDecoder(final ComponentLog logger) {
        this.logger = Objects.requireNonNull(logger, "Component Log required");
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf content, final List<Object> objects) throws Exception {
        logger.info("Processing record");

        final ParseFluent parser = new ParseFluent();

        InputStream in = new ByteBufInputStream(content);

        parser.setStream(in);
        parser.setLogger(logger);
        parser.parse();

        parser
            .getRecords()
            .stream()
            .map(r -> new MapRecord(FLUENT_RECORD_SCHEMA, Collections.unmodifiableMap(r.toMap())))
            .forEach(objects::add);
    }
}