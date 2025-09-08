package fi.hsl.transitdata.metro.ats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.metro.ats.Checks.checkEither;
import static fi.hsl.transitdata.metro.ats.RedisClusterProperties.redisClusterProperties;
import static redis.clients.jedis.Protocol.DEFAULT_DATABASE;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting transitdata-metro-ats-cancellation-source");
        final Config config = ConfigParser.createConfig();

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try (final PulsarApplication app = PulsarApplication.newInstance(config)) {
            final PulsarApplicationContext context = app.getContext();
            final JedisExecutor jedisExecutor = createJedisExecutor(context);
            final RedisUtils redis = new RedisUtils(jedisExecutor, context.getConfig().getInt("redis.ttlSeconds"));
            final int ttl = config.getInt("application.cacheTtlOffsetSeconds");
            final MetroCancellationFactory metroCancellationFactory = new MetroCancellationFactory(redis, ttl);
            final MessageHandler handler = new MessageHandler(context, metroCancellationFactory);

            final int repeatIntervalSeconds = config.getInt("application.repeatIntervalSeconds");
            log.info("Starting message repeating at {} seconds interval", repeatIntervalSeconds);
            scheduler.scheduleAtFixedRate(() -> {
                // TODO: is 1000 ok?
                final List<String> metroCancellationKeys = redis.getKeys(MetroCancellationFactory.REDIS_PREFIX_METRO_CANCELLATION, 1000);
                log.info("Found {} cached metro cancellation keys", metroCancellationKeys.size());
                final Map<String, Optional<Map<String, String>>> metroCancellationMaps = redis.getValuesByKeys(metroCancellationKeys);
                final List<String> dvjKeys = metroCancellationKeys.stream()
                        .map(key -> TransitdataProperties.REDIS_PREFIX_DVJ + key.split(":")[1])
                        .collect(Collectors.toList());
                final Map<String, Optional<Map<String, String>>> dvjMaps = redis.getValuesByKeys(dvjKeys);
                dvjMaps.forEach((k, maybeV) -> {
                    if (maybeV.isPresent()) {
                        final String dvjId = k.split(":")[1];
                        final String metroCancellationKey = MetroCancellationFactory.formatMetroCancellationKey(dvjId);
                        final Optional<Map<String, String>> maybeCachedMetroCancellation = metroCancellationMaps.get(metroCancellationKey);
                        if (maybeCachedMetroCancellation.isPresent()) {
                            final Map<String, String> metroCancellationMap = maybeCachedMetroCancellation.get();
                            final Map<String, String> dvjMap = maybeV.get();
                            boolean isValid = true;
                            if (!metroCancellationMap.containsKey(MetroCancellationFactory.KEY_CANCELLATION_STATUS)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached metro cancellation {}", MetroCancellationFactory.KEY_CANCELLATION_STATUS, metroCancellationKey);
                            }
                            if (!metroCancellationMap.containsKey(MetroCancellationFactory.KEY_TIMESTAMP)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached metro cancellation {}", MetroCancellationFactory.KEY_TIMESTAMP, metroCancellationKey);
                            }
                            if (!dvjMap.containsKey(TransitdataProperties.KEY_ROUTE_NAME)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached dvj data {}", TransitdataProperties.KEY_ROUTE_NAME, k);
                            }
                            if (!dvjMap.containsKey(TransitdataProperties.KEY_DIRECTION)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached dvj data {}", TransitdataProperties.KEY_DIRECTION, k);
                            }
                            if (!dvjMap.containsKey(TransitdataProperties.KEY_START_TIME)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached dvj data {}", TransitdataProperties.KEY_START_TIME, k);
                            }
                            if (!dvjMap.containsKey(TransitdataProperties.KEY_OPERATING_DAY)) {
                                isValid = false;
                                log.warn("Hash value for {} was not found for cached dvj data {}", TransitdataProperties.KEY_OPERATING_DAY, k);
                            }
                            if (!isValid) {
                                log.warn("Not producing repeated metro cancellation because hash value was not found");
                                return;
                            }
                            final String timestamp = metroCancellationMap.get(MetroCancellationFactory.KEY_TIMESTAMP);
                            long timestampLong;
                            try {
                                timestampLong = Long.parseLong(timestamp);
                            } catch (Exception e) {
                                log.warn("Not producing repeated metro cancellation because {} is not valid long", MetroCancellationFactory.KEY_TIMESTAMP);
                                return;
                            }
                            final String status = metroCancellationMap.get(MetroCancellationFactory.KEY_CANCELLATION_STATUS);
                            final String route = dvjMap.get(TransitdataProperties.KEY_ROUTE_NAME);
                            final String direction = dvjMap.get(TransitdataProperties.KEY_DIRECTION);
                            final String startTime = dvjMap.get(TransitdataProperties.KEY_START_TIME);
                            final String startDate = dvjMap.get(TransitdataProperties.KEY_OPERATING_DAY);
                            final Optional<InternalMessages.TripCancellation> maybeTripCancellation = MetroCancellationFactory.createTripCancellation(dvjId, route, direction, startTime, startDate, status);
                            if (maybeCachedMetroCancellation.isPresent()) {
                                final InternalMessages.TripCancellation cancellation = maybeTripCancellation.get();
                                handler.sendPulsarMessage(cancellation, timestampLong, dvjId);
                            } else {
                                log.warn("Not producing repeated metro cancellation because creating the message failed");
                                return;
                            }
                        } else {
                            log.warn("Cached metro cancellation for key {} was not found", metroCancellationKey);
                        }
                    } else {
                        log.warn("Cached DVJ data for key {} was not found", k);
                    }
                });
            }, 0, repeatIntervalSeconds, TimeUnit.SECONDS);

            log.info("Start handling the messages");
            app.launchWithHandler(handler);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }

    private static JedisExecutor createJedisExecutor(PulsarApplicationContext context) {
        final Config config = context.getConfig();
        final boolean redisEnabled = config.getBoolean("redis.enabled");
        final boolean redisClusterEnabled = config.getBoolean("redisCluster.enabled");
        checkEither(redisEnabled, redisClusterEnabled,
                "Exactly one of 'redis.enabled' or 'redisCluster.enabled' must be true");

        if (redisEnabled) {
            final Jedis jedis = context.getJedis();
            return new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    synchronized (jedis) {
                        return action.apply(jedis);
                    }
                }
            };
        } else {
            final RedisClusterProperties properties = redisClusterProperties(config);
            final JedisSentinelPool pool = createJedisSentinelPool(properties);
            final JedisExecutor jedisExecutor = new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    try (final Jedis jedis = pool.getResource()) {
                        return action.apply(jedis);
                    }
                }
            };

            if (properties.healthCheck) {
                context.getHealthServer()
                        .addCheck(redisCustomHealthCheck(jedisExecutor));
            }

            return jedisExecutor;
        }
    }

    private static JedisSentinelPool createJedisSentinelPool(RedisClusterProperties properties) {
        return new JedisSentinelPool(
                properties.masterName,
                properties.sentinels,
                properties.jedisPoolConfig(),
                (int) properties.connectionTimeout.toMillis(),
                (int) properties.socketTimeout.toMillis(),
                null,
                DEFAULT_DATABASE
        );
    }

    private static BooleanSupplier redisCustomHealthCheck(JedisExecutor jedisExecutor) {
        return () -> jedisExecutor.execute(jedis -> {
            try {
                final String maybePong = jedis.ping();
                if (maybePong.equals("PONG")) {
                    return true;
                } else {
                    log.error("jedis.ping() returned: {}", maybePong);
                }
            } catch (Exception e) {
                log.error("Exception in custom health check for redis connection", e);
            }

            return false;
        });
    }
}
