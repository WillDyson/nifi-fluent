package com.cloudera.ps.example.fluentReader;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DefaultSchedule(period = "25 ms")
@Tags({ "fluent", "logs", "logfiles", "record", "reader" })
@CapabilityDescription("Receive Fluent-formatted logs over TCP and write decoded Records.")
public class ListenFluent extends AbstractProcessor {
    static final String RECORD_COUNT = "record.count";

    static final PropertyDescriptor ADDRESS = new PropertyDescriptor.Builder()
            .name("Address")
            .displayName("Address")
            .description("Internet Protocol Address on which to listen for Fluent packets")
            .required(true)
            .defaultValue("0.0.0.0")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .displayName("Port")
            .description("UDP port number on which to listen for Fluent packets")
            .required(true)
            .defaultValue("1234")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor WORKER_THREADS = new PropertyDescriptor.Builder()
            .name("Worker Threads")
            .displayName("Worker Threads")
            .description("Number of threads responsible for decoding and queuing incoming Fluent packets")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor QUEUE_CAPACITY = new PropertyDescriptor.Builder()
            .name("Queue Capacity")
            .displayName("Queue Capacity")
            .description("Maximum number of Fluent Records that can be received and queued")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Maximum number of Fluent Records written for each FlowFile produced")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("Writer for serializing Fluent Records")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing one or more Fluent Records")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            ADDRESS,
            PORT,
            WORKER_THREADS,
            QUEUE_CAPACITY,
            BATCH_SIZE,
            RECORD_WRITER
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(SUCCESS);

    private static final RecordSchema FLUENT_RECORD_SCHEMA = new FluentRecordSchema();

    private volatile BlockingQueue<Record> records = new LinkedBlockingQueue<>();

    private EventServer eventServer;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws UnknownHostException {
        final int queueCapacity = context.getProperty(QUEUE_CAPACITY).asInteger();
        records = new LinkedBlockingQueue<>(queueCapacity);

        final String address = context.getProperty(ADDRESS).getValue();
        final InetAddress serverAddress = InetAddress.getByName(address);
        final int port = context.getProperty(PORT).asInteger();

        final FluentNettyEventServerFactory eventServerFactory = new FluentNettyEventServerFactory(serverAddress, port, getLogger(), records);
        eventServerFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));
        final int workerThreads = context.getProperty(WORKER_THREADS).asInteger();
        eventServerFactory.setWorkerThreads(workerThreads);

        try {
            eventServer = eventServerFactory.getEventServer();
        } catch (final EventException e) {
            getLogger().error("Server socket bind failed [{}:{}]", address, port, e);
        }
    }

    public int getListeningPort() {
        return eventServer.getListeningPort();
    }

    @OnStopped
    public void onStopped() {
        if (eventServer == null) {
            getLogger().info("Server not running");
        } else {
            eventServer.shutdown();
        }
        eventServer = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if (records.isEmpty()) {
            getLogger().trace("No Fluent Records queued");
            context.yield();
            return;
        } else {
            final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
            final List<Record> batchedRecords = new ArrayList<>(batchSize);
            records.drainTo(batchedRecords, batchSize);
            final Iterator<Record> recordBatch = batchedRecords.iterator();
            if (recordBatch.hasNext()) {
                final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                processRecords(session, recordBatch, writerFactory);
            }
        }
    }

    private void processRecords(final ProcessSession session, final Iterator<Record> recordBatch, final RecordSetWriterFactory writerFactory) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        FlowFile flowFile = session.create();
        try (
                OutputStream outputStream = session.write(flowFile);
                RecordSetWriter writer = writerFactory.createWriter(getLogger(), FLUENT_RECORD_SCHEMA, outputStream, Collections.emptyMap())
        ) {
            final PushBackRecordSet recordSet = getRecordSet(recordBatch);
            final Record record = recordSet.next();
            recordSet.pushback(record);

            final WriteResult writeResult = writer.write(recordSet);

            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
            attributes.putAll(writeResult.getAttributes());
            attributes.put(RECORD_COUNT, Integer.toString(writeResult.getRecordCount()));
        } catch (final Exception e) {
            getLogger().error("Write Fluent Records failed", e);
        }

        if (attributes.isEmpty()) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, SUCCESS);
        }
    }

    private PushBackRecordSet getRecordSet(final Iterator<Record> recordBatch) {
        final RecordSet recordSet = new RecordSet() {
            @Override
            public RecordSchema getSchema() {
                return FLUENT_RECORD_SCHEMA;
            }

            @Override
            public Record next() {
                return recordBatch.hasNext() ? recordBatch.next() : null;
            }
        };
        return new PushBackRecordSet(recordSet);
    }
}