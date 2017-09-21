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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.StringType;
import org.elasticsearch.common.inject.internal.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;

public class TypeExperiments {

    public static void main(String[] args) {

        ParameterDescriptor.ParameterType a = new ParameterDescriptor.ParameterType(
            ParameterDescriptor.ParameterType.Type.FIXED, type -> type instanceof StringType);


        ParameterDescriptor.ParameterType b = new ParameterDescriptor.ParameterType(
            ParameterDescriptor.ParameterType.Type.FIXED, type -> type instanceof IntegerType, DataTypes.INTEGER);

        ParameterDescriptor.ParameterType c = new ParameterDescriptor.ParameterType(
            ParameterDescriptor.ParameterType.Type.FIXED, type -> type.isNumeric());

        ParameterDescriptor descriptor = new ParameterDescriptor(a, a, b).withVarArg(c);

        List<DataType> argList = new ArrayList<>();
        argList.add(DataTypes.STRING);
        argList.add(DataTypes.STRING);
        argList.add(DataTypes.SHORT);
        argList.add(DataTypes.LONG);
        argList.add(DataTypes.LONG);
        argList.add(DataTypes.LONG);

        final List<DataType> newParams = descriptor.match(argList);
        if (newParams != null) {
            for (DataType dataType : newParams) {
                System.out.print(dataType + ", ");
            }
            System.out.println();
        }

    }


    private static class ParameterDescriptor {

        private final ParameterType[] parameters;

        // must come as last parameter
        private ParameterType varArgParameter;

        private ParameterDescriptor(ParameterType... parameters) {
            this.parameters = parameters;
        }

        public ParameterDescriptor withVarArg(ParameterType parameter) {
            this.varArgParameter = parameter;
            return this;
        }

        private static class ParameterType {

            enum Type {
                FIXED,
                CONVERTIBLE
            }

            private final Type type;
            private final Predicate<DataType> constraint;
            @Nullable
            private final DataType fallBack;

            private DataType boundType;
            private boolean isValidType;

            public ParameterType(Type type) {
                this(type, x -> true, null);
            }

            public ParameterType(Type type, Predicate<DataType> constraint) {
                this(type, constraint, null);
            }

            public ParameterType(Type type, Predicate<DataType> constraint, DataType fallBack) {
                this.type = type;
                this.constraint = constraint;
                this.fallBack = fallBack;
            }

            public DataType getBoundType(int argNum) {
                Preconditions.checkState(boundType != null,
                    "Type not bound when it should have been.");
                if (!isValidType) {
                    if (fallBack != null) {
                        DataType fallBackConversion = convertTypes(boundType, fallBack, constraint);
                        if (fallBackConversion != null) {
                            this.boundType = fallBackConversion;
                            this.isValidType = true;
                        }
                    } else {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ENGLISH,
                                "Parameter %s is of invalid type %s",
                                argNum, boundType));
                    }
                }
                return this.boundType;
            }

            private void bind(DataType dataType, int paramNum) {
                Objects.requireNonNull(dataType, "DataType to bind must not be null");
                if (boundType != null && boundType != dataType) {
                    switch (type) {
                        case FIXED:
                            throw new IllegalArgumentException(
                                String.format(
                                    Locale.ENGLISH,
                                    "Can't bind parameter %s to type %s, already bound to type %s.",
                                    paramNum, dataType, boundType));
                        case CONVERTIBLE:
                            DataType convertedType = convertTypes(boundType, dataType, constraint);
                            if (convertedType == null) {
                                throw new IllegalArgumentException(
                                    String.format(
                                        Locale.ENGLISH,
                                        "Can't convert type %s to type %s or vice-versa.",
                                        boundType, dataType));
                            }
                            this.boundType = convertedType;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown ParameterType.Type " + type);
                    }
                } else {
                    this.isValidType = constraint.test(dataType);
                    this.boundType = dataType;
                }
            }

            private void clear() {
                this.boundType = null;
            }

            /**
             * Tries to convert two {@link DataType} by respecting the precedence if possible.
             * For example, if given type A and type B, where A has higher precedence,
             * first try to cast B to A. If that doesn't work, try casting A to B.
             * @param type1 The first type given
             * @param type2 The second type given
             * @param constraint The constraint test to ensure type boundaries.
             * @return Either type1 or type2 depending on precedence and convertibility.
             */
            private static DataType convertTypes(DataType type1,
                                                 DataType type2,
                                                 Predicate<DataType> constraint) {
                final DataType targetType;
                final DataType sourceType;
                if (type1.precedes(type2)) {
                    targetType = type1;
                    sourceType = type2;
                } else {
                    targetType = type2;
                    sourceType = type1;
                }
                if (sourceType.isConvertableTo(targetType) && constraint.test(targetType)) {
                    return targetType;
                } else if (targetType.isConvertableTo(sourceType) && constraint.test(sourceType)) {
                    return sourceType;
                }
                return null;
            }
        }

        private void resetBoundParameters() {
            for (ParameterType parameter : parameters) {
                parameter.clear();
            }
        }

        public List<DataType> match(List<DataType> params) {
            Objects.requireNonNull(params);
            if (params.size() < parameters.length) {
                return null;
            }
            for (int i = 0; i < parameters.length; i++) {
                parameters[i].bind(params.get(i), i + 1);
            }
            int numberOfParameters = params.size();
            for (int i = parameters.length; i < numberOfParameters; i++) {
                varArgParameter.bind(params.get(i), i + 1);
            }
            List<DataType> newParams = new ArrayList<>(numberOfParameters);
            for (int i = 0; i < parameters.length; i++) {
                newParams.add(parameters[i].getBoundType(i + 1));
            }
            for (int i = parameters.length; i < numberOfParameters; i++) {
                newParams.add(varArgParameter.getBoundType(i + 1));
            }
            resetBoundParameters();
            return newParams;
        }



    }
}
