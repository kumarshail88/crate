/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.window;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

/**
 * The offset functions return the evaluated value at a row that
 * precedes or follows the current row by offset within its partition.
 * <p>
 * Synopsis:
 *      lag/lead(argument any [, offset integer [, default any ] ])
 */
public class OffsetValueFunctions implements WindowFunction {

    private abstract static class Implementation {
        protected Map<Integer, Row> indexToRow;
        protected int resolvedDirection;
        /* cachedNonNullIndex and idxInPartition forms an offset window containing 'offset' number of non-null elements and any number of nulls.
           The main perf. benefits will be seen when series of nulls are present.
           In the cases where idxInPartition is near the bounds that the cache cannot point to the valid index, it will hold a null. */
        protected Integer cachedNonNullIndex;

        protected abstract Object execute(int idxAtPartition,
                                          int offset,
                                          WindowFrameState currentFrame,
                                          List<? extends CollectExpression<Row, ?>> expressions,
                                          Input[] args);


        protected Object getValueAtOffsetIgnoringNulls(int idxInPartition,
                                                       int offset,
                                                       WindowFrameState currentFrame,
                                                       List<? extends CollectExpression<Row, ?>> expressions,
                                                       Input[] args) {
            if (cachedNonNullIndex == null) {
                cachedNonNullIndex = findNonNullOffsetFromCurrentIndex(idxInPartition,
                                                                       offset,
                                                                       currentFrame,
                                                                       expressions,
                                                                       args);
            } else {
                if (resolvedDirection > 0) {
                    var curValue = getValueAtTargetIndex(idxInPartition, currentFrame, expressions, args);
                    if (curValue != null) {
                        moveCacheToNextNonNull(currentFrame, expressions, args);
                    }
                } else {
                    var prevValue = getValueAtTargetIndex(idxInPartition - 1, currentFrame, expressions, args);
                    if (prevValue != null) {
                        moveCacheToNextNonNull(currentFrame, expressions, args);
                    }
                }
            }
            return getValueAtTargetIndex(cachedNonNullIndex, currentFrame, expressions, args);
        }

        protected void moveCacheToNextNonNull(WindowFrameState currentFrame,
                                              List<? extends CollectExpression<Row, ?>> expressions,
                                              Input[] args) {
            if (cachedNonNullIndex == null) {
                return;
            }
            /* from cached index, search from left to right for a non-null element.
               for 'lag', cache index will never go past idxInPartition(current index).
               for 'lead', cache index may reach upperBoundExclusive. */
            for (int i = cachedNonNullIndex + 1; i <= currentFrame.upperBoundExclusive(); i++) {
                if (i == currentFrame.upperBoundExclusive()) {
                    cachedNonNullIndex = null;
                    break;
                }
                Object value = getValueAtTargetIndex(i, currentFrame, expressions, args);
                if (value != null) {
                    cachedNonNullIndex = i;
                    break;
                }
            }
        }

        protected Integer findNonNullOffsetFromCurrentIndex(int idxInPartition,
                                                            int offset,
                                                            WindowFrameState currentFrame,
                                                            List<? extends CollectExpression<Row, ?>> expressions,
                                                            Input[] args) {
            /* Search for 'offset' number of non-null elements in 'resolvedDirection' and return the index if found.
               If index goes out of bound, an exception will be thrown and eventually invoke getDefaultOrNull(...) */
            for (int i = 1, counter = 0; ; i++) {
                Object value = getValueAtTargetIndex(getTargetIndex(idxInPartition, i),
                                                     currentFrame,
                                                     expressions,
                                                     args);
                if (value != null) {
                    counter++;
                    if (counter == offset) {
                        return getTargetIndex(idxInPartition, i);
                    }
                }
            }
        }

        protected Object getValueAtTargetIndex(Integer targetIndex,
                                               WindowFrameState currentFrame,
                                               List<? extends CollectExpression<Row, ?>> expressions,
                                               Input[] args) {
            if (targetIndex == null) {
                throw new IndexOutOfBoundsException();
            }
            Row row;
            if (indexToRow.containsKey(targetIndex)) {
                row = indexToRow.get(targetIndex);
            } else {
                Object[] rowCells = currentFrame.getRowInPartitionAtIndexOrNull(targetIndex);
                if (rowCells == null) {
                    throw new IndexOutOfBoundsException();
                }
                row = new RowN(rowCells);
                indexToRow.putIfAbsent(targetIndex, row);
            }
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(row);
            }
            return args[0].value();
        }

        protected int getTargetIndex(int idxInPartition, int offset) {
            return idxInPartition + offset * resolvedDirection;
        }
    }

    private static class RespectNullsImpl extends Implementation {
        @Override
        public Object execute(int idxAtPartition,
                              int offset,
                              WindowFrameState currentFrame,
                              List<? extends CollectExpression<Row, ?>> expressions,
                              Input[] args) {
            var targetIndex = getTargetIndex(idxAtPartition, offset);
            return getValueAtTargetIndex(targetIndex, currentFrame, expressions, args);
        }
    }

    private static class IgnoreNullsImpl extends Implementation {
        @Override
        public Object execute(int idxAtPartition,
                              int offset,
                              WindowFrameState currentFrame,
                              List<? extends CollectExpression<Row, ?>> expressions,
                              Input[] args) {
            if (offset == 0) {
                throw new IllegalArgumentException("offset 0 is not a valid argument if ignore nulls flag is set");
            }
            return getValueAtOffsetIgnoringNulls(idxAtPartition, offset, currentFrame, expressions, args);
        }
    }

    private static final String LAG_NAME = "lag";
    private static final String LEAD_NAME = "lead";
    private static final int LAG_DEFAULT_OFFSET = -1;
    private static final int LEAD_DEFAULT_OFFSET = 1;
    private final int directionMultiplier;
    private final Implementation implementation;


    private final Signature signature;
    private final Signature boundSignature;
    private Integer cachedOffset;

    private OffsetValueFunctions(Signature signature,
                                 Signature boundSignature,
                                 int directionMultiplier,
                                 Implementation implementation) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.directionMultiplier = directionMultiplier;
        this.implementation = implementation;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Object execute(int idxInPartition,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          Input[] args) {
        if (idxInPartition == 0) {
            implementation.indexToRow = new HashMap<>(currentFrame.size());
        }
        final int newOffset;
        if (args.length > 1) {
            Object offsetValue = args[1].value();
            if (offsetValue != null) {
                newOffset = ((Number) offsetValue).intValue();
            } else {
                return null;
            }
        } else {
            newOffset = 1;
        }
        // if the offset is changed compared to the previous iteration, cache will be cleared and offset/direction re-calculated
        if (idxInPartition == 0 || (cachedOffset != null && cachedOffset != newOffset)) {
            if (newOffset != 0) {
                implementation.resolvedDirection = newOffset / Math.abs(newOffset) * directionMultiplier;
            }
            implementation.cachedNonNullIndex = null;
        }
        cachedOffset = newOffset;
        final int cachedOffsetMagnitude = Math.abs(cachedOffset);
        try {
            return implementation.execute(idxInPartition, cachedOffsetMagnitude, currentFrame, expressions, args);
        } catch (IndexOutOfBoundsException e) {
            return getDefaultOrNull(args);
        }
    }

    private static Object getDefaultOrNull(Input[] args) {
        if (args.length == 3) {
            return args[2].value();
        } else {
            return null;
        }
    }

    public static void register(ExtraFunctionsModule module) {
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );

        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );

        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );

        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.FALSE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new RespectNullsImpl()
                )
        );

        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );

        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E"))
                .withIgnoreNulls(Boolean.TRUE),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET,
                    new IgnoreNullsImpl()
                )
        );
    }
}
