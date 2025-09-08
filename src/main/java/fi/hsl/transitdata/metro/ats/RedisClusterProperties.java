package fi.hsl.transitdata.metro.ats;

import com.typesafe.config.Config;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

import static fi.hsl.transitdata.metro.ats.Checks.checkNotEmpty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

public class RedisClusterProperties {

    private static final Duration DEFAULT_IDLE_CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_MAX_WAIT = Duration.ofMillis(500);
    private static final int DEFAULT_MIN_IDLE_CONNECTIONS = 12;
    private static final int DEFAULT_MAX_CONNECTIONS = 16;
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(500);
    private static final Duration DEFAULT_SOCKET_TIMEOUT = Duration.ofSeconds(500);

    public final String masterName;
    public final Set<String> sentinels;
    public final boolean healthCheck;
    public final Duration idleConnectionTimeout;
    public final int minIdleConnections;
    public final int maxConnections;
    public final Duration maxWait;
    public final Duration connectionTimeout;
    public final Duration socketTimeout;

    private RedisClusterProperties(String masterName,
                                   Set<String> sentinels,
                                   @Nullable Boolean healthCheck,
                                   @Nullable Duration idleConnectionTimeout,
                                   @Nullable Integer minIdleConnections,
                                   @Nullable Integer maxConnections,
                                   @Nullable Duration maxWait,
                                   @Nullable Duration connectionTimeout,
                                   @Nullable Duration socketTimeout) {
        this.masterName = checkNotEmpty("masterName", masterName);
        this.sentinels = checkNotEmpty("sentinels", sentinels);
        this.healthCheck = ofNullable(healthCheck).orElse(false);
        this.idleConnectionTimeout = ofNullable(idleConnectionTimeout).orElse(DEFAULT_IDLE_CONNECTION_TIMEOUT);
        this.minIdleConnections = ofNullable(minIdleConnections).orElse(DEFAULT_MIN_IDLE_CONNECTIONS);
        this.maxConnections = ofNullable(maxConnections).orElse(DEFAULT_MAX_CONNECTIONS);
        this.maxWait = ofNullable(maxWait).orElse(DEFAULT_MAX_WAIT);
        this.connectionTimeout = ofNullable(connectionTimeout).orElse(DEFAULT_CONNECTION_TIMEOUT);
        this.socketTimeout = ofNullable(socketTimeout).orElse(DEFAULT_SOCKET_TIMEOUT);
    }

    public static RedisClusterProperties redisClusterProperties(Config config) {
        return new RedisClusterProperties(
                config.getString("redisCluster.masterName"),
                Arrays.stream(config.getString("redisCluster.sentinels").split(","))
                        .collect(toSet()),
                config.hasPath("redisCluster.healthCheck")
                        ? config.getBoolean("redisCluster.healthCheck")
                        : null,
                config.hasPath("redisCluster.idleConnectionTimeout")
                        ? config.getDuration("redisCluster.idleConnectionTimeout")
                        : null,
                config.hasPath("redisCluster.minIdleConnections")
                        ? config.getInt("redisCluster.minIdleConnections")
                        : null,
                config.hasPath("redisCluster.maxConnections")
                        ? config.getInt("redisCluster.maxConnections")
                        : null,
                config.hasPath("redisCluster.maxWait")
                        ? config.getDuration("redisCluster.maxWait")
                        : null,
                config.hasPath("redisCluster.connectionTimeout")
                        ? config.getDuration("redisCluster.connectionTimeout")
                        : null,
                config.hasPath("redisCluster.socketTimeout")
                        ? config.getDuration("redisCluster.socketTimeout")
                        : null
        );
    }

    public JedisPoolConfig jedisPoolConfig() {
        final JedisPoolConfig config = new JedisPoolConfig();
        config.setMinEvictableIdleTime(idleConnectionTimeout);
        config.setMinIdle(minIdleConnections);
        config.setMaxIdle(maxConnections);
        config.setMaxTotal(maxConnections);
        config.setTestOnBorrow(false);
        config.setTestOnCreate(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(true);
        config.setMaxWait(maxWait);
        return config;
    }
}
