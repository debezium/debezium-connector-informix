package io.debezium.connector.informix;

import java.io.Serializable;
import java.util.stream.StreamSupport;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

import com.informix.stream.api.IfmxStreamRecord;

import io.debezium.DebeziumException;

public class IfxCacheEntryExpiredListener implements Serializable, CacheEntryExpiredListener<Long, IfmxStreamRecord> {
    public IfxCacheEntryExpiredListener() {
    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends Long, ? extends IfmxStreamRecord>> cacheEntryEvents) throws CacheEntryListenerException {
        throw new DebeziumException("Transaction cache entries expired: %s".formatted(
                StreamSupport.stream(cacheEntryEvents.spliterator(), true).map(CacheEntryEvent::getKey).map(Long::valueOf).sorted()));
    }
}
