package com.cloudera.ps.example.fluentReader;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.apache.nifi.logging.ComponentLog;
import org.msgpack.core.MessageInsufficientBufferException;

import java.util.List;
import java.util.Objects;

public class MessagePackFrameDecoder extends ByteToMessageDecoder {
    private final ComponentLog logger;

    public MessagePackFrameDecoder(final ComponentLog logger) {
        this.logger = Objects.requireNonNull(logger, "Component Log required");
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf content, List<Object> objects) throws Exception {
        logger.debug("Decoding frame");

        if (!content.isReadable()) {
            return;
        }

        int messageLength = -1;

        try {
            content.markReaderIndex();
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(content.nioBuffer());
            unpacker.skipValue();
            messageLength = (int) unpacker.getTotalReadBytes();
            content.resetReaderIndex();
        } catch (MessageInsufficientBufferException e) {
            content.resetReaderIndex();
            return;
        }

        if (messageLength > 0 && content.readableBytes() >= messageLength) {
            logger.debug("Found complete frame");

            ByteBuf frame = content.readBytes(messageLength);
            objects.add(frame);
        }
    }
}