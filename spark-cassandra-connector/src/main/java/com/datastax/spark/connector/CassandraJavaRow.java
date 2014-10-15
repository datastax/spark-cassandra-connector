package com.datastax.spark.connector;

import com.datastax.spark.connector.types.TypeConverter;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public interface CassandraJavaRow {
    /**
     * Total number of columns in this row. Includes columns with null values.
     */
    int length();

    /**
     * Returns true if column value is Cassandra null
     */
    boolean isNullAt(int index);

    /**
     * Returns true if column value is Cassandra null
     */
    boolean isNullAt(String name);

    /**
     * Returns index of column with given name or -1 if column not found
     */
    int indexOf(String name);

    /**
     * Returns the name of the i-th column.
     */
    String nameOf(int idx);

    /**
     * Returns true if column with given name is defined and has an
     * entry in the underlying value array, i.e. was requested in the result set.
     * For columns having null value, returns true.
     */
    boolean contains(String name);

    /**
     * Converts this row to a Map
     */
    Map<String, Object> toJMap();

    /**
     * Generic getter for getting columns of any type.
     * Looks the column up by its index. First column starts at index 0.
     */
    <T> T getJ(int idx, TypeConverter<T> typeConverter);

    /**
     * Generic getter for getting columns of any type.
     * Looks the column up by column name. Column names are case-sensitive.
     */
    <T> T getJ(String name, TypeConverter<T> typeConverter);

    /**
     * Returns a column value without applying any conversion.
     * The underlying type is the same as the type returned by the low-level Cassandra driver.
     * May return Java null.
     */
    Object get(int idx);

    /**
     * Returns a column value without applying any conversion.
     * The underlying type is the same as the type returned by the low-level Cassandra driver.
     * May return Java null.
     */
    Object get(String name);

    Integer getJInt(int idx);

    Integer getJInt(String name);

    Byte getJByte(int idx);

    Byte getJByte(String name);

    Short getJShort(int idx);

    Short getJShort(String name);

    Double getJDouble(int idx);

    Double getJDouble(String name);

    Float getJFloat(int idx);

    Float getJFloat(String name);

    Long getJLong(int idx);

    Long getJLong(String name);

    Boolean getJBoolean(int idx);

    Boolean getJBoolean(String name);

    BigInteger getJVarInt(int idx);

    BigInteger getJVarInt(String name);

    BigDecimal getJDecimal(int idx);

    BigDecimal getJDecimal(String name);

    Date getDate(int idx);

    Date getDate(String name);

    String getString(int idx);

    String getString(String name);

    ByteBuffer getBytes(int idx);

    ByteBuffer getBytes(String name);

    UUID getUUID(int idx);

    UUID getUUID(String name);

    InetAddress getInet(int idx);

    InetAddress getInet(String name);

    DateTime getDateTime(int idx);

    DateTime getDateTime(String name);

    <T> List<T> getJList(int idx);

    <T> List<T> getJList(String name);

    <T> Set<T> getJSet(int idx);

    <T> Set<T> getJSet(String name);

    <K, V> Map<K, V> getJMap(int idx);

    <K, V> Map<K, V> getJMap(String name);

}
