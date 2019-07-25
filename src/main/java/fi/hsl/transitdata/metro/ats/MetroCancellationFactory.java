package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.redis.RedisUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MetroCancellationFactory {
    private static final Logger log = LoggerFactory.getLogger(MetroCancellationFactory.class);

    public static final String REDIS_PREFIX_METRO_CANCELLATION = "metro-cancellation:";
    public static final String KEY_CANCELLATION_STATUS = "cancellation-status";
    public static final String KEY_TIMESTAMP = "timestamp";

    private final RedisUtils redis;
    private final int cacheTtlOffsetSeconds;

    public MetroCancellationFactory(final RedisUtils redis, final int cacheTtlOffsetSeconds) {
        this.redis = redis;
        this.cacheTtlOffsetSeconds = cacheTtlOffsetSeconds;
    }

    public Optional<InternalMessages.TripCancellation> toTripCancellation(final Message message) {
        try {

            final long timestamp = message.getEventTime();
            // TODO: check if journey was cancelled previously but not anymore.
            return toTripCancellation(message.getData(), timestamp);

        } catch (Exception e) {
            log.warn("Failed to produce metro estimate trip cancellation", e);
        }

        return Optional.empty();
    }

    private Optional<InternalMessages.TripCancellation> toTripCancellation(byte[] data, long timestamp) throws Exception {
        MetroAtsProtos.MetroEstimate metroEstimate = MetroAtsProtos.MetroEstimate.parseFrom(data);
        final String dvjId = metroEstimate.getDvjId();
        final InternalMessages.TripCancellation.Status status = getCancellationStatus(metroEstimate);

        // Check cache
        final String metroCancellationKey = formatMetroCancellationKey(dvjId);
        if (status.equals(InternalMessages.TripCancellation.Status.CANCELED)) {
            setCacheValue(metroEstimate, metroCancellationKey, status, timestamp);
        } else {
            final Optional<Map<String, String>> maybeCachedMetroCancellation = redis.getValues(metroCancellationKey);
            if (maybeCachedMetroCancellation.isPresent()) {
                // This is cancellation of cancellation
                final Map<String, String> cachedMetroCancellation = maybeCachedMetroCancellation.get();
                if (cachedMetroCancellation.containsKey(KEY_CANCELLATION_STATUS)) {
                    final InternalMessages.TripCancellation.Status cachedStatus = InternalMessages.TripCancellation.Status.valueOf(cachedMetroCancellation.get(KEY_CANCELLATION_STATUS));
                    // Only update cache if trip was previously cancelled
                    if (cachedStatus.equals(InternalMessages.TripCancellation.Status.CANCELED)) {
                        setCacheValue(metroEstimate, metroCancellationKey, status, timestamp);
                    }
                } else {
                    log.warn("Hash value for {} is missing for cached metro cancellation {}", KEY_CANCELLATION_STATUS, metroCancellationKey);
                    return Optional.empty();
                }
            } else {
                // Ignoring because this is neither cancellation nor cancellation of cancellation
                return Optional.empty();
            }
        }

        return createTripCancellation(
                dvjId,
                metroEstimate.getRouteName(),
                metroEstimate.getDirection(),
                metroEstimate.getStartTime(),
                metroEstimate.getOperatingDay(),
                status
        );
    }

    public static Optional<InternalMessages.TripCancellation> createTripCancellation(final String dvjId,
                                                                                     final String route,
                                                                                     final String direction,
                                                                                     final String startTime,
                                                                                     final String startDate,
                                                                                     final String status) {
        if (!validateString(KEY_CANCELLATION_STATUS, status)) {
            return Optional.empty();
        }
        InternalMessages.TripCancellation.Status statusEnum;
        try {
            statusEnum = InternalMessages.TripCancellation.Status.valueOf(status);
        } catch (Exception e) {
            log.warn("{} is not valid cancellation status", KEY_CANCELLATION_STATUS);
            return Optional.empty();
        }
        return createTripCancellation(dvjId, route, direction, startTime, startDate, statusEnum);
    }

    public static Optional<InternalMessages.TripCancellation> createTripCancellation(final String dvjId,
                                                                                     final String route,
                                                                                     final String direction,
                                                                                     final String startTime,
                                                                                     final String startDate,
                                                                                     final InternalMessages.TripCancellation.Status status) {
        boolean isValid = true;
        int directionInt;
        isValid &= validateString(TransitdataProperties.KEY_DVJ_ID, dvjId);
        isValid &= validateString(TransitdataProperties.KEY_ROUTE_NAME, route);
        isValid &= validateString(TransitdataProperties.KEY_DIRECTION, direction);
        try {
            directionInt = Integer.parseInt(direction);
        } catch (Exception e) {
            log.warn("{} is not valid integer", TransitdataProperties.KEY_DIRECTION);
            return Optional.empty();
        }
        Integer.parseInt(direction);
        isValid &= validateString(TransitdataProperties.KEY_START_TIME, startTime);
        isValid &= validateString(TransitdataProperties.KEY_OPERATING_DAY, startDate);
        if (!isValid) {
            return Optional.empty();
        }

        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());
        builder.setTripId(dvjId);
        builder.setRouteId(route);
        builder.setDirectionId(directionInt);
        builder.setStartTime(startTime);
        builder.setStartDate(startDate);
        builder.setStatus(status);
        return Optional.of(builder.build());
    }

    private static boolean validateString(final String key, final String value) {
        final boolean isValid = value != null && !value.isEmpty();
        if (!isValid) {
            log.warn("{} is not valid string");
        }
        return isValid;
    }

    public static String formatMetroCancellationKey(final String dvjId) {
        return REDIS_PREFIX_METRO_CANCELLATION + dvjId;
    }

    private int getCacheTtlSeconds(final MetroAtsProtos.MetroEstimate metroEstimate) {
        final String endDateTime = metroEstimate.getEndTime();
        final long endMillis = Instant.parse(endDateTime).toEpochMilli();
        final long now = System.currentTimeMillis();
        final long cacheTtlSeconds = ((endMillis - now) / 1000) + cacheTtlOffsetSeconds;
        return (int) cacheTtlSeconds;
    }

    private InternalMessages.TripCancellation.Status getCancellationStatus(final MetroAtsProtos.MetroEstimate metroEstimate) {
        final MetroAtsProtos.MetroProgress progress = metroEstimate.getJourneySectionprogress();
        return progress.equals(MetroAtsProtos.MetroProgress.CANCELLED) ?
                InternalMessages.TripCancellation.Status.CANCELED :
                InternalMessages.TripCancellation.Status.RUNNING;
    }

    private void setCacheValue(final MetroAtsProtos.MetroEstimate metroEstimate, final String key, final InternalMessages.TripCancellation.Status status, final long timestamp) {
        final int cacheTtlSeconds = getCacheTtlSeconds(metroEstimate);
        if (cacheTtlSeconds > 0) {
            final Map<String, String> data = new HashMap<>();
            data.put(KEY_CANCELLATION_STATUS, status.toString());
            data.put(KEY_TIMESTAMP, String.valueOf(timestamp));
            final String response = redis.setExpiringValues(key, data, cacheTtlSeconds);
            if (!redis.checkResponse(response)) {
                log.error("Failed to set key {} into cache", key);
            }
        } else {
            log.warn("Not setting key {} into cache because TTL is negative {}", key, cacheTtlSeconds);
            // TODO: something might be wrong here because this trip should have ended already.
            // TODO: should return Optional.empty() because this trip has already ended?
        }
    }
}
