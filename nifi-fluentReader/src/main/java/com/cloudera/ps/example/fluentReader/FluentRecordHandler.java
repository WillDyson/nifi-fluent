package com.cloudera.ps.example.fluentReader;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.EventExecutor;
import org.apache.nifi.event.transport.EventDroppedException;
import org.apache.nifi.serialization.record.Record;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
public class FluentRecordHandler extends SimpleChannelInboundHandler<Record> {
    private static final long OFFER_TIMEOUT = 500;

    private final BlockingQueue<Record> records;

    public FluentRecordHandler(final BlockingQueue<Record> records) {
        this.records = Objects.requireNonNull(records, "Records Queue required");
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final Record record) {
        final SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();

        final EventExecutor eventExecutor = channelHandlerContext.executor();
        while (!offer(record, remoteAddress)) {
            if (eventExecutor.isShuttingDown()) {
                throw new EventDroppedException(String.format("Dropped Record from Sender [%s] executor shutting down", remoteAddress));
            }
        }
    }

    private boolean offer(final Record record, final SocketAddress remoteAddress) {
        try {
            return records.offer(record, OFFER_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventDroppedException(String.format("Dropped Record from Sender [%s]", remoteAddress), e);
        }
    }
}