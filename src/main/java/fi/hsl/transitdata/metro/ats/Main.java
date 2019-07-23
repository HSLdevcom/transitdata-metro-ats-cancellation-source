package fi.hsl.transitdata.metro.ats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.redis.RedisUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting transitdata-metro-ats-cancellation-source");
        final Config config = ConfigParser.createConfig();

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try (final PulsarApplication app = PulsarApplication.newInstance(config)) {
            final PulsarApplicationContext context = app.getContext();
            final RedisUtils redis = RedisUtils.newInstance(context);
            final int ttl = config.getInt("application.cacheTtlOffsetSeconds");
            final MetroCancellationFactory metroCancellationFactory = new MetroCancellationFactory(redis, ttl);
            final MessageHandler handler = new MessageHandler(context, metroCancellationFactory);

            final int repeatIntervalSeconds = config.getInt("application.repeatIntervalSeconds");
            log.info("Starting message repeating at {} seconds interval");
            scheduler.scheduleAtFixedRate(() -> {
                // TODO: is 1000 ok?
                final List<String> metroCancellationKeys = redis.getKeys(MetroCancellationFactory.REDIS_PREFIX_METRO_CANCELLATION, 1000);
                log.info("Found {} keys in scheduled task", metroCancellationKeys.size());
                final Map<String, Optional<String>> metroCancellationMaps = redis.getValueBykeys(metroCancellationKeys);
                final List<String> dvjKeys = metroCancellationKeys.stream()
                        .map(key -> TransitdataProperties.REDIS_PREFIX_DVJ + key.split(":")[1])
                        .collect(Collectors.toList());
                final Map<String, Optional<Map<String, String>>> dvjMaps = redis.getValuesByKeys(dvjKeys);
                dvjMaps.forEach((k, maybeV) -> {
                    if (maybeV.isPresent()) {
                        final String dvjId = k.split(":")[1];
                        final String metroCancellationKey = MetroCancellationFactory.formatMetroCancellationKey(dvjId);
                        final Optional<String> maybeCachedStatus = metroCancellationMaps.get(metroCancellationKey);
                        if (maybeCachedStatus.isPresent()) {
                            final InternalMessages.TripCancellation.Status cachedStatus = InternalMessages.TripCancellation.Status.valueOf(maybeCachedStatus.get());
                            final Map<String, String> dvjValue = maybeV.get();
                        } else {
                            log.warn("Metro cancellation value for key {} was not found in cache", metroCancellationKey);
                        }
                    } else {
                        log.warn("Dvj value for key {} was not found in cache", k);
                    }
                });
            }, 0, repeatIntervalSeconds, TimeUnit.SECONDS);

            log.info("Start handling the messages");
            app.launchWithHandler(handler);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}
