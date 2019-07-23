package fi.hsl.transitdata.metro.ats;

import fi.hsl.common.redis.RedisUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
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
        InternalMessages.TripCancellation.Status status;

        // Check cache
        final String metroCancellationKey = formatMetroCancellationKey(dvjId);
        final MetroAtsProtos.MetroProgress progress = metroEstimate.getJourneySectionprogress();
        if (progress.equals(MetroAtsProtos.MetroProgress.CANCELLED)) {
            status = InternalMessages.TripCancellation.Status.CANCELED;
            setCacheValue(metroEstimate, metroCancellationKey, status, timestamp);
        } else {
            final Optional<Map<String, String>> maybeCachedMetroCancellation = redis.getValues(metroCancellationKey);
            if (maybeCachedMetroCancellation.isPresent()) {
                // This is cancellation of cancellation
                status = InternalMessages.TripCancellation.Status.RUNNING;
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

        // TripCancellation
        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());
        builder.setTripId(dvjId);
        builder.setRouteId(metroEstimate.getRouteName());
        builder.setDirectionId(Integer.parseInt(metroEstimate.getDirection()));
        builder.setStartTime(metroEstimate.getStartTime());
        builder.setStartDate(metroEstimate.getOperatingDay());
        builder.setStatus(status);

        return Optional.of(builder.build());
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

    private void setCacheValue(final MetroAtsProtos.MetroEstimate metroEstimate, final String key, final InternalMessages.TripCancellation.Status value, final long timestamp) {
        final int cacheTtlSeconds = getCacheTtlSeconds(metroEstimate);
        if (cacheTtlSeconds > 0) {
            final String response = redis.setExpiringValue(key, value.toString(), cacheTtlSeconds);
            if (!redis.checkResponse(response)) {
                log.error("Failed to put key {} with value {} into cache", key, value);
            }
        } else {
            log.warn("Not putting key {} in into cache because TTL is negative {}", key, cacheTtlSeconds);
            // TODO: something might be wrong here because this trip should have ended already.
            // TODO: should return Optional.empty() because this trip has already ended?
        }
    }
}
