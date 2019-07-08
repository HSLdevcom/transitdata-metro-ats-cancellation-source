package fi.hsl.transitdata.metro.ats;

import com.fasterxml.jackson.databind.ObjectMapper;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.metro.ats.models.MetroSchedule;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class CancellationFactory {
    private static final Logger log = LoggerFactory.getLogger(CancellationFactory.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Pattern datetimeFloorPattern = Pattern.compile("\\d{2}\\.\\d{3}Z$");

    private Jedis jedis;

    public CancellationFactory(final PulsarApplicationContext context) {
        this.jedis = context.getJedis();
    }

    public Optional<InternalMessages.TripCancellation> toCancellation(final Message message) {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(message);
            if (maybeSchema.isPresent()) {
                final byte[] data = message.getData();
                final Mqtt.RawMessage mqttMessage = Mqtt.RawMessage.parseFrom(data);
                final byte[] payload = mqttMessage.getPayload().toByteArray();
                Optional<MetroSchedule> maybeSchedule = parsePayload(payload);

                if (maybeSchedule.isPresent()) {
                    final MetroSchedule schedule = maybeSchedule.get();
                    // TODO: check if journey was cancelled previously but not anymore
                    final InternalMessages.TripCancellation cancellation = toCancellation(schedule);
                    return Optional.of(cancellation);
                } else {
                    throw new Exception("Failed to parse metro schedule");
                }
            }
        } catch (Exception e) {
            log.warn("Failed to produce metro schedule stop estimates", e);
        }
        return Optional.empty();
    }

    private InternalMessages.TripCancellation toCancellation(final MetroSchedule schedule) throws Exception {
        final String[] shortNames = schedule.routeName.split("-");
        if (shortNames.length != 2) {
            throw new IllegalArgumentException(String.format("Failed to parse metro schedule route name %s", schedule.routeName));
        }
        final String startStopShortName = shortNames[0];
        final String startDatetime = floorDateTime(schedule.beginTime);
        final String metroId = TransitdataProperties.formatMetroId(startStopShortName, startDatetime);

        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());

        Optional<Map<String, String>> metroJourneyData = getMetroJourneyData(metroId);
        metroJourneyData.ifPresent(map -> {
            if (map.containsKey(TransitdataProperties.KEY_DVJ_ID))
                builder.setTripId(map.get(TransitdataProperties.KEY_DVJ_ID));
            if (map.containsKey(TransitdataProperties.KEY_OPERATING_DAY))
                builder.setStartDate(map.get(TransitdataProperties.KEY_OPERATING_DAY));
            if (map.containsKey(TransitdataProperties.KEY_ROUTE_NAME))
                builder.setRouteId(map.get(TransitdataProperties.KEY_ROUTE_NAME));
            if (map.containsKey(TransitdataProperties.KEY_DIRECTION))
                builder.setDirectionId(Integer.parseInt(map.get(TransitdataProperties.KEY_DIRECTION)));
            if (map.containsKey(TransitdataProperties.KEY_START_TIME))
                builder.setStartTime(map.get(TransitdataProperties.KEY_START_TIME));
        });

        switch (schedule.journeySectionprogress) {
            case SCHEDULED:
            case INPROGRESS:
            case COMPLETED:
                builder.setStatus(InternalMessages.TripCancellation.Status.RUNNING);
                break;
            case CANCELLED:
                builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);
                break;
            default:
                log.warn("Unrecognized status {}", schedule.journeySectionprogress);
                break;
        }

        return builder.build();
    }

    private String floorDateTime(final String dateTime) {
        return datetimeFloorPattern.matcher(dateTime).replaceFirst("00.000Z");
    }

    private Optional<Map<String, String>> getMetroJourneyData(final String metroId) {
        return Optional.ofNullable(jedis.hgetAll(metroId));
    }

    private Optional<MetroSchedule> parsePayload(final byte[] payload) {
        try {
            MetroSchedule schedule = mapper.readValue(payload, MetroSchedule.class);
            return Optional.of(schedule);
        } catch (Exception e) {
            log.warn(String.format("Failed to parse payload %s", new String(payload)), e);
        }
        return Optional.empty();
    }
}
