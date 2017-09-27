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

package io.crate.metadata;

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.FuncArg;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import io.crate.types.UndefinedType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * TODO mxm
 */
public class FuncParams {


    public static final ParamType ANY_PARAM_TYPE = ParamType.of();
    public static final ParamType NUMERIC_PARAM_TYPE = ParamType.of(DataTypes.NUMERIC_PRIMITIVE_TYPES);
    public static final ParamType ANY_ARRAY_PARAM_TYPE = ParamType.of().matchBaseType(ArrayType.class);
    public static final ParamType INTEGER_PARAM_TYPE = ParamType.of(IntegerType.INSTANCE);
    public static final ParamType STRING_PARAM_TYPE = ParamType.of(StringType.INSTANCE);
    public static final ParamType BOOLEAN_PARAM_TYPE = ParamType.of(BooleanType.INSTANCE);

    public static final FuncParams NONE = FuncParams.of();
    public static final FuncParams SINGLE_ANY = FuncParams.of(ANY_PARAM_TYPE);
    public static final FuncParams SINGLE_NUMERIC = FuncParams.of(NUMERIC_PARAM_TYPE);

//    public static void main(String[] args) {
//
//        ParamType a = ParamType.of(StringType.INSTANCE);
//        ParamType b = ParamType.of(DataTypes.INTEGER);
//        ParamType c = ParamType.of(DataTypes.NUMERIC_PRIMITIVE_TYPES);
//
//        FuncParams descriptor = FuncParams.of(a, a, b).withVarArgs(c);
//
//        List<Symbol> argList = new ArrayList<>();
//        argList.add(Literal.of("test"));
//        argList.add(Literal.of("test"));
//        argList.add(Literal.of("test"));
//        argList.add(Literal.of(1L));
//        argList.add(Literal.of(1L));
//        argList.add(Literal.of(1.2));
//        argList.add(Literal.of(1L));
//
//        final List<DataType> newParams = descriptor.match(argList);
//        if (newParams != null) {
//            for (Symbol dataType : newParams) {
//                System.out.print(dataType + ", ");
//            }
//            System.out.println();
//        }
//
//    }

    private final ParamType[] parameters;

    private ParamType[] varArgParameters;
    private int maxVarArgOccurrences;

    private FuncParams(ParamType... parameters) {
        this.parameters = parameters;
    }

    public static FuncParams of(ParamType... parameters) {
        return new FuncParams(parameters);
    }

    public static FuncParams of(Collection<DataType> fixedSignature) {
        ParamType[] params = (ParamType[]) fixedSignature.stream().map(ParamType::new).toArray();
        return of(params);
    }

    /**
     * Adds an optional and variable number of occurrences of the
     * following parameters.
     * @param parameters The types used in the var arg parameters.
     * @return FuncParams
     */
    public FuncParams withVarArgs(ParamType... parameters) {
        return withVarArgs(-1, parameters);
    }

    /**
     * Adds an optional and a fixed upper number of occurrences of the
     * following parameters.
     * @param parameters The types used in the var arg parameters.
     * @return FuncParams
     */
    public FuncParams withVarArgs(int limitOfOccurrences, ParamType... parameters) {
        // TODO mxm make calling this only possible once!!
        this.varArgParameters = parameters;
        this.maxVarArgOccurrences = limitOfOccurrences;
        return this;
    }

    private void resetBoundTypes() {
        for (ParamType parameter : parameters) {
            parameter.clear();
        }
        for (ParamType parameter : varArgParameters) {
            parameter.clear();
        }
    }

    private FuncArg cast(FuncArg funcArg, DataType dataType) {
        // TODO mxm
        return funcArg;
    }

    private void bindParams(List<? extends FuncArg> params) {
        for (int i = 0; i < parameters.length; i++) {
            FuncArg funcArg = params.get(i);
            parameters[i].bind(funcArg, i + 1);
        }
        int numberOfParameters = params.size();
        int remainingArgs = numberOfParameters - parameters.length;
        int remainingChecks = maxVarArgOccurrences == -1 ?
            varArgParameters.length : varArgParameters.length * maxVarArgOccurrences;
        if (remainingArgs - remainingChecks > 0) {
            throw new IllegalArgumentException("The number of arguments is incorrect: " + params);
        }
        for (int i = parameters.length; i < numberOfParameters; i++) {
            for (int k = 0; k < varArgParameters.length; k++) {
                FuncArg funcArg = params.get(i + k);
                varArgParameters[k].bind(funcArg, i + 1);
            }
        }
    }

    private List<DataType> retrieveBoundParams(List<? extends FuncArg> params) {
        int numberOfParameters = params.size();
        List<DataType> newParams = new ArrayList<>(numberOfParameters);
        for (int i = 0; i < parameters.length; i++) {
            DataType boundType = parameters[i].getBoundType();
//            FuncArg castedArg = cast(params.get(i), boundType);
            newParams.add(boundType);
        }
        for (int i = parameters.length; i < numberOfParameters; i++) {
            for (int k = 0; k < varArgParameters.length; k++) {
                DataType boundType = varArgParameters[k].getBoundType();
//                FuncArg castSymbol = cast(params.get(i + k), boundType);
                newParams.add(boundType);
            }
        }
        resetBoundTypes();
        return newParams;
    }

    /**
     * TODO mxm
     * @param params
     * @return
     */
    public List<DataType> match(List<? extends FuncArg> params) {
        Objects.requireNonNull(params, "Supplied parameter types may not be null.");
        if (params.size() < parameters.length) {
            return null;
        }
        bindParams(params);
        return retrieveBoundParams(params);
    }

    public static class ParamType {

        private final SortedSet<DataType> validTypes;

        private DataType boundType;
        private Class<? extends DataType> matchBaseType;

        private ParamType(DataType... validTypes) {
            this(Collections.emptyList(), validTypes);
        }

        private ParamType(Collection<DataType> validTypes, DataType... validTypes2) {
            this.validTypes = new TreeSet<>((o1, o2) -> {
                if (o1.precedes(o2)) {
                    return -1;
                } else if (o2.precedes(o1)) {
                    return 1;
                }
                return 0;
            });
            this.validTypes.addAll(validTypes);
            this.validTypes.addAll(Arrays.asList(validTypes2));
        }

        public static ParamType of(DataType... dataType) {
            return new ParamType(dataType);
        }

        public static ParamType of(Collection<DataType> dataTypes, DataType... dataTypes2) {
            return new ParamType(dataTypes, dataTypes2);
        }

        public ParamType matchBaseType(Class<? extends DataType> baseClazz) {
            // TODO mxm make calling this only possible once
            this.matchBaseType = baseClazz;
            return this;
        }

        public DataType getBoundType() {
            Preconditions.checkState(boundType != null,
                "Type not bound when it should have been.");
            return this.boundType;
        }

        private void bind(FuncArg funcArg, int paramNum) {
            Objects.requireNonNull(funcArg, "funcArg to bind must not be null");
            DataType dataType = Objects.requireNonNull(funcArg.valueType(),
                "Provided funcArg type must not be null");
            if (boundType != null) {
                if (boundType.equals(dataType)) {
                    return;
                } else if (matchBaseType != null && matchBaseType.isAssignableFrom(dataType.getClass())) {
                    return;
                }
                DataType convertedType = null;
                if (funcArg.canBeCasted()) {
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
            } else {
                if (!validTypes.isEmpty() && !validTypes.contains(dataType)) {
                    DataType convertedType = null;
                    if (funcArg.canBeCasted()) {
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
