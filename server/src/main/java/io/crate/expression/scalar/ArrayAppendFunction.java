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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

import java.util.ArrayList;
import java.util.List;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

class ArrayAppendFunction extends Scalar<List<Object>, Object> {

    public static final String NAME = "array_append";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("E"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            ArrayAppendFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<?> innerType;

    ArrayAppendFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.innerType = ((ArrayType<?>) boundSignature.getReturnType().createType()).innerType();
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
    public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        ArrayList<Object> resultList = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) args[0].value();
        Object valueToAdd = args[1].value();
        if (values != null) {
            for (Object value : values) {
                resultList.add(innerType.sanitizeValue(value));
            }
        }
        resultList.add(innerType.sanitizeValue(valueToAdd));
        return resultList;
    }
}
