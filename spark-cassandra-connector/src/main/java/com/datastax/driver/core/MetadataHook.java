package com.datastax.driver.core;

import java.nio.ByteBuffer;

/**
 * Wraps the driver's metadata to add the ability to build tokens from partition keys.
 * <p/>
 * This class has to live in {@code com.datastax.driver.core} in order to access package-private fields.
 */
public class MetadataHook {
    /**
     * Builds a new {@link Token} from a partition key, according to the partitioner reported by the Cassandra nodes.
     *
     * @param metadata               the original driver's metadata.
     * @param routingKey             the routing key of the bound partition key
     * @return the token.
     * @throws IllegalStateException if the token factory was not initialized. This would typically
     *                               happen if metadata was explicitly disabled with {@link QueryOptions#setMetadataEnabled(boolean)}
     *                               before startup.
     */
    public static Token newToken(Metadata metadata, ByteBuffer routingKey) {
        return metadata.tokenFactory().hash(routingKey);
    }
}
