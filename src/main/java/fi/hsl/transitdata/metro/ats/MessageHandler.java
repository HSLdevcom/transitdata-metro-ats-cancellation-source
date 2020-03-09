package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class MessageHandler implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private MetroCancellationFactory metroCancellationFactory;

    public MessageHandler(final PulsarApplicationContext context, final MetroCancellationFactory metroCancellationFactory) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.metroCancellationFactory = metroCancellationFactory;
    }

    public void handleMessage(final Message received) throws Exception {
        try {
            final Optional<InternalMessages.TripCancellation> maybeTripCancellation = metroCancellationFactory.toTripCancellation(received);
            if (maybeTripCancellation.isPresent()) {
                final InternalMessages.TripCancellation cancellation = maybeTripCancellation.get();
                final MessageId messageId = received.getMessageId();
                final long timestamp = received.getEventTime();
                sendPulsarMessage(messageId, cancellation, timestamp, cancellation.getTripId());
            } else {
                // Metro-estimate status was not CANCELED, not sending message.
                ack(received.getMessageId()); //Ack so we don't receive it again
            }
        } catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private void ack(final MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    public void sendPulsarMessage(final InternalMessages.TripCancellation cancellation, final long timestamp, final String dvjId) {
        sendPulsarMessage(null, cancellation, timestamp, dvjId);
    }

    public void sendPulsarMessage(final MessageId received, final InternalMessages.TripCancellation cancellation, final long timestamp, final String dvjId) {
        producer.newMessage()
                .key(dvjId)
                .eventTime(timestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                .property(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(cancellation.getSchemaVersion()))
                .property(TransitdataProperties.KEY_DVJ_ID, dvjId) // TODO remove once TripUpdateProcessor won't need it anymore
                .value(cancellation.toByteArray())
                .sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Should we abort?
                    }
                    else {
                        log.info("Produced a cancellation for trip: " + cancellation.getRouteId() + "/" +
                                cancellation.getDirectionId() + "-" + cancellation.getStartTime() + "-" +
                                cancellation.getStartDate() + " with status: " + cancellation.getStatus().toString());
                        //Does this become a bottleneck? Does pulsar send more messages before we ack the previous one?
                        //If yes we need to get rid of this
                        if (received != null) {
                            ack(received);
                        }
                    }
                });
    }
}
