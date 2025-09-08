package fi.hsl.transitdata.metro.ats;

import redis.clients.jedis.Jedis;

import java.util.function.Function;

@FunctionalInterface
public interface JedisExecutor {

    <T> T execute(Function<Jedis, T> action);
}
