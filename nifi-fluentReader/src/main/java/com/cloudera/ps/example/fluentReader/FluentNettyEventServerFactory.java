package com.cloudera.ps.example.fluentReader;

import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

public class FluentNettyEventServerFactory extends NettyEventServerFactory {
    private static final int TCP_PACKET_MAXIMUM_SIZE = 65507;

    public FluentNettyEventServerFactory(
            final InetAddress address,
            final int port,
            final ComponentLog log,
            final BlockingQueue<Record> records
    ) {
        super(address, port, TransportProtocol.TCP);
        setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        setSocketReceiveBuffer(TCP_PACKET_MAXIMUM_SIZE);

        final FluentPacketDecoder fluentPacketDecoder = new FluentPacketDecoder(log);
        final FluentRecordHandler fluentRecordHandler = new FluentRecordHandler(records);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);

        setHandlerSupplier(() -> Arrays.asList(
            new MessagePackFrameDecoder(log),
            fluentPacketDecoder,
            fluentRecordHandler,
            logExceptionChannelHandler
        ));
    }
}