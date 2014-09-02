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
 * A "simple" (not-truly-composite) Composite.
 * Copy of toByteBuffer() implementation only from cassandra
 */
public class SimpleComposite extends AbstractComposite
{

    protected final ByteBuffer element;

    public SimpleComposite(ByteBuffer element)
    {
        // We have to be careful with empty ByteBuffers as we shouldn't store them.
        // To avoid errors (and so isEmpty() works as we intend), we don't allow simpleComposite with
        // an empty element (but it's ok for CompoundComposite, it's a row marker in that case).
        assert element.hasRemaining();
        this.element = element;
    }

    public int size()
    {
        return 1;
    }

    public ByteBuffer get(int i)
    {
        if (i != 0)
            throw new IndexOutOfBoundsException();

        return element;
    }


    @Override
    public ByteBuffer toByteBuffer()
    {
        return element;
    }

}
