/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.spark.connector.util;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

/**
 * Copy of toByteBuffer() implementation only from cassandra
 */
public abstract class AbstractComposite implements Composite
{
    public static final int STATIC_MARKER = 0xFFFF;

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean isStatic()
    {
        return false;
    }


    public ByteBuffer toByteBuffer()
    {
        // This is the legacy format of composites.
        // See org.apache.cassandra.db.marshal.CompositeType for details.
        ByteBuffer result = ByteBuffer.allocate(dataSize() + 3 * size() + (isStatic() ? 2 : 0));
        if (isStatic())
            ByteBufferUtil.writeShortLength(result, STATIC_MARKER);

        for (int i = 0; i < size(); i++)
        {
            ByteBuffer bb = get(i);
            ByteBufferUtil.writeShortLength(result, bb.remaining());
            result.put(bb.duplicate());
            result.put((byte)0);
        }
        result.flip();
        return result;
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

}
