package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.metro.ats.models.CancellationData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class MetroAtsCancellationHandler/* implements IMessageHandler*/ {
    /*
    static final Logger log = LoggerFactory.getLogger(MetroAtsCancellationHandler.class);

    String timeZone;
    private Producer<byte[]> producer;

    enum OMMAffectedDeparturesStatus {
        active, deleted
    }

    public void handleMessage(final Message message) throws Exception {

    }

    public MetroAtsCancellationHandler(PulsarApplicationContext context) {
        producer = context.getProducer();
        timeZone = context.getConfig().getString("application.timezone");
    }

    public Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, timeZone);
    }

    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();

        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        }
        catch (Exception e) {
            log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }

    public void handleAndSend(ResultSet resultSet) throws SQLException, PulsarClientException {
        List<CancellationData> cancellations = parseData(resultSet);
        cancellations = filterDuplicates(cancellations);
        sendCancellations(cancellations);
    }

    private List<CancellationData> parseData(ResultSet resultSet) throws SQLException {
        List<CancellationData> cancellations = new LinkedList<>();
        while (resultSet.next()) {
            try {
                InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();

                String routeId = resultSet.getString("ROUTE_NAME");
                builder.setRouteId(routeId);
                int joreDirection = resultSet.getInt("DIRECTION");
                builder.setDirectionId(joreDirection);
                String startDate = resultSet.getString("OPERATING_DAY"); // yyyyMMdd
                builder.setStartDate(startDate);
                String starTime = resultSet.getString("START_TIME"); // HH:mm:ss in local time
                builder.setStartTime(starTime);

                String adStatus = resultSet.getString("AFFECTED_DEPARTURES_STATUS");
                // If active -> cancellation is valid, if deleted then the cancellation has been cancelled.
                if (adStatus != null && OMMAffectedDeparturesStatus.valueOf(adStatus.toLowerCase()) == OMMAffectedDeparturesStatus.deleted) {
                    log.debug("Cancelling a cancellation for route {}:{}:{}:{}", routeId, startDate, starTime, joreDirection);
                    builder.setStatus(InternalMessages.TripCancellation.Status.RUNNING);
                }
                else {
                    builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);
                }

                //Version number is defined in the proto file as default value but we still need to set it since it's a required field
                builder.setSchemaVersion(builder.getSchemaVersion());
                final String dvjId = Long.toString(resultSet.getLong("DVJ_ID"));
                builder.setTripId(dvjId);

                final InternalMessages.TripCancellation cancellation = builder.build();

                log.debug("Read cancellation for route {} with dvjId {}",
                        routeId, dvjId);

                Timestamp timestamp = resultSet.getTimestamp("AFFECTED_DEPARTURES_LAST_MODIFIED"); //other option is to use DEVIATION_CASES_LAST_MODIFIED
                Optional<Long> epochTimestamp = toUtcEpochMs(timestamp.toString());
                if (!epochTimestamp.isPresent()) {
                    log.error("Failed to parse epoch timestamp from resultset: {}", timestamp.toString());
                }
                else {
                    CancellationData data = new CancellationData(cancellation, epochTimestamp.get(), dvjId);
                    cancellations.add(data);
                }
            }
            catch (IllegalArgumentException iae) {
                log.error("Error while parsing the cancellation resultset", iae);
            }
        }
        return cancellations;
    }

    static List<CancellationData> filterDuplicates(List<CancellationData> cancellations) {
        List<CancellationData> filtered = new LinkedList<>();

        // Having even one active cancellation means that the trip is cancelled (or actually we should always have either 1 or 0).

        // Cancelling a cancelled cancellation can produce us duplicate rows in the data, if this is done multiple times.
        // We need to find out if there's more than one rows per dvjId. If that is so we can deduct which is the correct one to send.
        Map<String, List<CancellationData>> groupedByDvjId = cancellations.stream().collect(Collectors.groupingBy(CancellationData::getDvjId));
        for(Map.Entry<String, List<CancellationData>> entry: groupedByDvjId.entrySet()) {
            List<CancellationData> dataForThisDvjId = entry.getValue();

            Map<InternalMessages.TripCancellation.Status, List<CancellationData>> groupedByStatus = dataForThisDvjId
                    .stream()
                    .collect(Collectors.groupingBy(data -> data.payload.getStatus()));

            if (groupedByStatus.containsKey(InternalMessages.TripCancellation.Status.CANCELED)) {
                //Cancellation always wins, there should be always only one of these
                List<CancellationData> activeCancellations = groupedByStatus.get(InternalMessages.TripCancellation.Status.CANCELED);
                if (activeCancellations.size() != 1) {
                    log.warn("Something strange in OMM, more than one active cancellation");
                }
                filtered.add(activeCancellations.get(0));
            }
            else if (groupedByStatus.containsKey(InternalMessages.TripCancellation.Status.RUNNING)){
                // Let's pick the latest, although doesn't really matter since these just represent cancellation of cancellation,
                // no matter how many times it has been cancelled
                List<CancellationData> cancelledCancellations = groupedByStatus.get(InternalMessages.TripCancellation.Status.RUNNING);
                cancelledCancellations.sort(Comparator.comparingLong(CancellationData::getTimestamp));
                filtered.add(cancelledCancellations.get(0));
            }
            else {
                log.error("This is impossible, found Cancellation which is neither canceled or running!");
            }
        }

        return filtered;
    }

    private void sendCancellations(List<CancellationData> cancellations) throws PulsarClientException {
        for (CancellationData data: cancellations) {
            sendPulsarMessage(data.payload, data.timestampEpochMs, data.dvjId);
        }
    }

    private void sendPulsarMessage(InternalMessages.TripCancellation tripCancellation, long timestamp, String dvjId) throws PulsarClientException {
        try {
            producer.newMessage().value(tripCancellation.toByteArray())
                    .eventTime(timestamp)
                    .key(dvjId)
                    .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                    .send();

            log.info("Produced a cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                    tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                    tripCancellation.getStartDate());

        }
        catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        }
        catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }
    }
    */
}
