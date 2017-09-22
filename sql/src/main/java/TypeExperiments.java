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

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * TODO mxm
 */
public class TypeExperiments {

    public static void main(String[] args) {

        ParameterType a = new ParameterType(ParameterType.Type.CONVERTIBLE, StringType.INSTANCE);


        ParameterDescriptor.ParameterType b = new ParameterDescriptor.ParameterType(
            ParameterDescriptor.ParameterType.Type.FIXED, DataTypes.INTEGER);

        ParameterDescriptor.ParameterType c = new ParameterDescriptor.ParameterType(
            ParameterDescriptor.ParameterType.Type.CONVERTIBLE, DataTypes.NUMERIC_PRIMITIVE_TYPES);

        ParameterDescriptor descriptor = new ParameterDescriptor(a, a, b).withVarArg(c);

        List<Symbol> argList = new ArrayList<>();
        argList.add(Literal.of("test"));
        argList.add(Literal.of("test"));
        argList.add(Literal.of("test"));
        argList.add(Literal.of(1L));
        argList.add(Literal.of(1L));
        argList.add(Literal.of(1.2));
        argList.add(Literal.of(1L));

        final List<Symbol> newParams = descriptor.match(argList);
        if (newParams != null) {
            for (Symbol dataType : newParams) {
                System.out.print(dataType + ", ");
            }
            System.out.println();
        }

    }


    private static class ParameterDescriptor {

        private final ParameterType[] parameters;

        // varargs are always the last item in the parameter list
        private ParameterType varArgParameter;

        private ParameterDescriptor(ParameterType... parameters) {
            this.parameters = parameters;
        }

        public ParameterDescriptor withVarArg(ParameterType parameter) {
            this.varArgParameter = parameter;
            return this;
        }

        private void resetBoundTypes() {
            for (ParameterType parameter : parameters) {
                parameter.clear();
            }
            varArgParameter.clear();
        }

        private Symbol cast(Symbol symbol, DataType dataType) {
            return symbol;
        }

        private void bindParams(List<Symbol> params) {
            for (int i = 0; i < parameters.length; i++) {
                Symbol symbol = params.get(i);
                parameters[i].bind(symbol, i + 1);
            }
            int numberOfParameters = params.size();
            for (int i = parameters.length; i < numberOfParameters; i++) {
                Symbol symbol = params.get(i);
                varArgParameter.bind(symbol, i + 1);
            }
        }

        private List<Symbol> retrieveBoundParams(List<Symbol> params) {
            int numberOfParameters = params.size();
            List<Symbol> newParams = new ArrayList<>(numberOfParameters);
            for (int i = 0; i < parameters.length; i++) {
                DataType boundType = parameters[i].getBoundType();
                Symbol castSymbol = cast(params.get(i), boundType);
                newParams.add(castSymbol);
            }
            for (int i = parameters.length; i < numberOfParameters; i++) {
                DataType boundType = varArgParameter.getBoundType();
                Symbol castSymbol = cast(params.get(i), boundType);
                newParams.add(castSymbol);
            }
            resetBoundTypes();
            return newParams;
        }

        public List<Symbol> match(List<Symbol> params) {
            Objects.requireNonNull(params, "Supplied parameter types may not be null.");
            if (params.size() < parameters.length) {
                return null;
            }
            bindParams(params);
            return retrieveBoundParams(params);
        }

    }

    private static class ParameterType {

        enum Type {
            // Can only be assigned a type once
            FIXED,
            // Can be converted from one type to another type
            CONVERTIBLE
        }

        private final Type type;
        private final SortedSet<DataType> validTypes;

        private DataType boundType;
        private boolean isColumn;

        public ParameterType(Type type) {
            this(type, new DataType[]{});
        }

        public ParameterType(Type type, Collection<DataType> validTypes) {
            this (type, validTypes.toArray(new DataType[]{}));
        }

        private ParameterType(Type type, DataType... validTypes) {
            this.type = type;
            this.validTypes = new TreeSet<>((o1, o2) -> {
                if (o1.precedes(o2)) {
                    return -1;
                } else if (o2.precedes(o1)) {
                    return 1;
                }
                return 0;
            });
            this.validTypes.addAll(Arrays.asList(validTypes));
        }

        public static ParameterType fixed(DataType dataType) {
            return new ParameterType(Type.FIXED, dataType);
        }

        public static ParameterType fixed(DataType dataType) {
            return new ParameterType(Type.FIXED, dataType);
        }

        public DataType getBoundType() {
            Preconditions.checkState(boundType != null,
                "Type not bound when it should have been.");
            return this.boundType;
        }

        private void bind(Symbol symbol, int paramNum) {
            Objects.requireNonNull(symbol, "Symbol to bind must not be null");
            DataType dataType = Objects.requireNonNull(symbol.valueType(),
                "Provided symbol type must not be null");
            if (boundType != null && boundType != dataType) {
                switch (type) {
                    case FIXED:
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ENGLISH,
                                "Can't bind parameter %s to type %s, already bound to type %s.",
                                paramNum, dataType, boundType));
                    case CONVERTIBLE:
                        DataType convertedType = null;
                        if (!isColumn) {
                            convertedType = convertTypes(dataType, boundType);
                        }
                        if (convertedType == null) {
                            throw new IllegalArgumentException(
                                String.format(
                                    Locale.ENGLISH,
                                    "Can't convert type %s to type %s or vice-versa for parameter %s.",
                                    dataType, boundType, paramNum));
                        }
                        this.boundType = convertedType;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown ParameterType.Type " + type);
                }
            } else {
                this.isColumn = isColumn(symbol);
                if (!validTypes.isEmpty() && !validTypes.contains(dataType)) {
                    DataType convertedType = null;
                    if (!isColumn) {
                        convertedType = convert(dataType, validTypes.first());
                    }
                    if (convertedType == null) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ENGLISH,
                                "Parameter %s is of invalid type %s",
                                paramNum, boundType));
                    }
                    this.boundType = convertedType;
                } else {
                    this.boundType = dataType;
                }
            }
        }

        private static boolean isColumn(Symbol symbol) {
            return SymbolVisitors.any(s -> s instanceof Field, symbol);
        }

        private void clear() {
            this.boundType = null;
        }

        private static DataType convert(DataType source, DataType target) {
            if (source.isConvertableTo(target)) {
                return target;
            }
            return null;
        }

        /**
         * Tries to convert two {@link DataType} by respecting the precedence if possible.
         * For example, if given type A and type B, where A has higher precedence,
         * first try to cast B to A. If that doesn't work, try casting A to B.
         * @param type1 The first type given
         * @param type2 The second type given
         * @return Either type1 or type2 depending on precedence and convertibility.
         */
        private DataType convertTypes(DataType type1, DataType type2) {
            final DataType targetType;
            final DataType sourceType;
            if (type1.precedes(type2)) {
                targetType = type1;
                sourceType = type2;
            } else {
                targetType = type2;
                sourceType = type1;
            }
            if (sourceType.isConvertableTo(targetType) && validTypes.contains(targetType)) {
                return targetType;
            } else if (targetType.isConvertableTo(sourceType) && validTypes.contains(sourceType)) {
                return sourceType;
            }
            return null;
        }
    }

}
