/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import io.crate.Streamer;

import java.util.Objects;

/**
 * Wrapper type of Literal's, i.e. concrete values and not columns.
 *
 * Literal types are not final until they are converted (casted) to
 * match a type of a column. This happens upon
 *
 * 1) function calls, e.g.
 *
 * add(IntegerType, LiteralType) -> add(IntegerType, IntegerType) -> IntegerType
 *
 * 2) inserts/updates
 *
 * create table t1 (id integer);
 * insert into t1 values (LiteralType) -> insert into t1 values (IntegerType)
 *
 */
public class LiteralType extends DataType {

    public static final int ID = 42;

    private final DataType innerType;

    public LiteralType(DataType innerType) {
        this.innerType = Objects.requireNonNull(innerType, "Inner type must not be null");
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.LiteralType;
    }

    @Override
    public String getName() {
        return "literal_" + innerType.getName();
    }

    @Override
    public Streamer<?> streamer() {
        throw new IllegalStateException("LiteralType may not be streamed.");
    }

    @Override
    public Object value(Object value) throws IllegalArgumentException, ClassCastException {
        return innerType.value(value);
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        //noinspection unchecked
        return innerType.compareValueTo(val1, val2);
    }
}
