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

import java.nio.ByteBuffer;

/**
 * A "truly-composite" Composite.
 * Copy of toByteBuffer() implementation only from cassandra
 */
public class CompoundComposite extends AbstractComposite
{
    // We could use a List, but we'll create such object *a lot* and using a array+size is not
    // all that harder, so we save the List object allocation.
    final ByteBuffer[] elements;
    final int size;
    final boolean isStatic;

    public CompoundComposite(ByteBuffer[] elements, int size, boolean isStatic)
    {
        this.elements = elements;
        this.size = size;
        this.isStatic = isStatic;
    }

    public int size()
    {
        return size;
    }

    public ByteBuffer get(int i)
    {
        // Note: most consumer should validate that i is within bounds. However, for backward compatibility
        // reasons, composite dense tables can have names that don't have all their component of the clustering
        // columns, which may end up here with i > size(). For those calls, it's actually simpler to return null
        // than to force the caller to special case.
        return i >= size() ? null : elements[i];
    }

    @Override
    public boolean isStatic()
    {
        return isStatic;
    }

}
