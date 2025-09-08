package fi.hsl.transitdata.metro.ats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class RedisUtils {

    private static final Logger log = LoggerFactory.getLogger(RedisUtils.class);

    private final JedisExecutor jedisExecutor;
    private final int ttlSeconds;

    public RedisUtils(JedisExecutor jedisExecutor, int ttlSeconds) {
        this.jedisExecutor = jedisExecutor;
        this.ttlSeconds = ttlSeconds;
        log.info("Redis TTL: {} seconds", ttlSeconds);
    }

    public String setValue(String key, String value) {
        return jedisExecutor.execute(jedis -> jedis.set(key, value));
    }

    public String setExpiringValue(String key, String value) {
        return this.setExpiringValue(key, value, ttlSeconds);
    }

    public String setExpiringValue(String key, String value, int ttlInSeconds) {
        return jedisExecutor.execute(jedis -> jedis.setex(key, ttlInSeconds, value));
    }

    public String setValues(String key, Map<String, String> values) {
        return jedisExecutor.execute(jedis -> jedis.hmset(key, values));
    }

    public String setExpiringValues(String key, Map<String, String> values) {
        return setExpiringValues(key, values, ttlSeconds);
    }

    public String setExpiringValues(String key, Map<String, String> values, int ttlInSeconds) {
        String response = this.setValues(key, values);
        setExpire(key, ttlInSeconds);
        return response;
    }

    public Long setExpire(String key) {
        return setExpire(key, ttlSeconds);
    }

    public Long setExpire(String key, int ttlInSeconds) {
        return jedisExecutor.execute(jedis -> jedis.expire(key, ttlInSeconds));
    }

    public Optional<String> getValue(String key) {
        return jedisExecutor.execute(jedis -> {
            String value = jedis.get(key);
            return value != null && !value.isEmpty() ? Optional.ofNullable(value) : Optional.empty();
        });
    }

    public Optional<Map<String, String>> getValues(String key) {
        return jedisExecutor.execute(jedis -> {
            Map<String, String> values = jedis.hgetAll(key);
            return values != null && !values.isEmpty() ? Optional.ofNullable(values) : Optional.empty();
        });
    }

    public List<String> getKeys(String prefix, Integer count) {
        return this.getKeys(prefix, "*", count);
    }

    public List<String> getKeys(String prefix, String pattern, Integer count) {
        ScanParams scanParams = new ScanParams();
        scanParams.match(prefix + pattern);
        scanParams.count(count);
        HashSet<String> keys = new HashSet<>();

        return jedisExecutor.execute(jedis -> {
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> result = scanResult.getResult();
                keys.addAll(result);
                cursor = scanResult.getCursor();
            } while (!"0".equals(cursor));

            return new ArrayList<>(keys);
        });
    }

    public Map<String, Optional<Map<String, String>>> getValuesByKeys(List<String> keys) {
        return jedisExecutor.execute(jedis -> {
            Transaction transaction = jedis.multi();
            Map<String, Response<Map<String, String>>> responses = new HashMap();
            keys.forEach((key) -> {
                responses.put(key, transaction.hgetAll(key));
            });
            transaction.exec();
            Map<String, Optional<Map<String, String>>> values = new HashMap(responses.size());
            responses.forEach((k, v) -> {
                Map<String, String> value = (Map) v.get();
                if (value != null && !value.isEmpty()) {
                    values.put(k, Optional.of(value));
                } else {
                    values.put(k, Optional.empty());
                }

            });
            return values;
        });
    }

    public Map<String, Optional<String>> getValueBykeys(List<String> keys) {
        return jedisExecutor.execute(jedis -> {
            Transaction transaction = jedis.multi();
            Map<String, Response<String>> responses = new HashMap();
            keys.forEach((key) -> {
                responses.put(key, transaction.get(key));
            });
            transaction.exec();
            Map<String, Optional<String>> values = new HashMap(responses.size());
            responses.forEach((k, v) -> {
                String value = (String) v.get();
                if (value != null && !value.isEmpty()) {
                    values.put(k, Optional.of(value));
                } else {
                    values.put(k, Optional.empty());
                }

            });
            return values;
        });
    }

    public String updateTimestamp() {
        return jedisExecutor.execute(jedis -> {
            OffsetDateTime now = OffsetDateTime.now();
            String ts = DateTimeFormatter.ISO_INSTANT.format(now);
            log.info("Updating Redis timestamp to {}", ts);
            return jedis.set("cache-update-ts", ts);
        });
    }

    public boolean checkResponse(String response) {
        return response != null && response.trim().equalsIgnoreCase("OK");
    }

    public boolean checkResponse(Long response) {
        return response != null && response == 1L;
    }
}