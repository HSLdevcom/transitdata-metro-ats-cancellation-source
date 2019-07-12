package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class MetroCancellationFactory {
    private static final Logger log = LoggerFactory.getLogger(MetroCancellationFactory.class);

    public Optional<InternalMessages.TripCancellation> toTripCancellation(final Message message) {
        try {

            final long timestamp = message.getEventTime();
            // TODO: check if journey was cancelled previously but not anymore
            return toTripCancellation(message.getData(), timestamp);

        } catch (Exception e) {
            log.warn("Failed to produce metro estimate trip cancellation", e);
        }

        return Optional.empty();
    }

    private Optional<InternalMessages.TripCancellation> toTripCancellation(byte[] data, final long timestamp) throws Exception {
        MetroAtsProtos.MetroEstimate metroEstimate = MetroAtsProtos.MetroEstimate.parseFrom(data);

        // TripCancellation
        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());
        builder.setTripId(metroEstimate.getDvjId());
        builder.setRouteId(metroEstimate.getRouteName());
        builder.setDirectionId(Integer.parseInt(metroEstimate.getDirection()));
        builder.setStartTime(metroEstimate.getStartTime());
        builder.setStartDate(metroEstimate.getOperatingDay());
        if (!metroEstimate.getJourneySectionprogress().equals(MetroAtsProtos.MetroProgress.CANCELLED)) {
            return Optional.empty();
        }
        builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);

        return Optional.of(builder.build());
    }
}
