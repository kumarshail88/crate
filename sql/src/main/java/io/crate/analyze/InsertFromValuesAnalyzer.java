/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Function;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.symbol.*;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReferenceInfo;
import io.crate.metadata.ReferenceToLiteralConverter;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.Input;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.ValuesList;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.*;

@Singleton
public class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer {

    private static final ReferenceToLiteralConverter TO_LITERAL_CONVERTER = new ReferenceToLiteralConverter();

    private static class ValuesResolver implements io.crate.analyze.ValuesAwareExpressionAnalyzer.ValuesResolver {

        private final DocTableRelation tableRelation;
        public List<Reference> columns;
        public List<String> assignmentColumns;
        public Object[] insertValues;

        public ValuesResolver(DocTableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }

        @Override
        public Symbol allocateAndResolve(Field argumentColumn) {
            // use containsKey instead of checking result .get() for null because inserted value might actually be null
            Reference columnReference = tableRelation.resolveField(argumentColumn);
            if (!columns.contains(columnReference)) {
                throw new IllegalArgumentException(String.format(
                        "Referenced column '%s' isn't part of the column list of the INSERT statement",
                        argumentColumn.path().outputName()));
            }
            assert columnReference != null;
            DataType returnType = columnReference.valueType();
            assignmentColumns.add(columnReference.ident().columnIdent().fqn());
            return Literal.newLiteral(returnType, returnType.value(insertValues[columns.indexOf(columnReference)]));
        }
    }

    @Inject
    protected InsertFromValuesAnalyzer(AnalysisMetaData analysisMetaData) {
        super(analysisMetaData);
    }

    @Override
    public AbstractInsertAnalyzedStatement visitInsertFromValues(InsertFromValues node, Analysis analysis) {
        DocTableInfo tableInfo = analysisMetaData.referenceInfos().getWritableTable(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);

        FieldProvider fieldProvider = new NameFieldProvider(tableRelation);
        ExpressionAnalyzer expressionAnalyzer =
                new ExpressionAnalyzer(analysisMetaData, analysis.parameterContext(), fieldProvider, tableRelation);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        expressionAnalyzer.resolveWritableFields(true);

        ValuesResolver valuesResolver = new ValuesResolver(tableRelation);
        ExpressionAnalyzer valuesAwareExpressionAnalyzer = new ValuesAwareExpressionAnalyzer(
                analysisMetaData, analysis.parameterContext(), fieldProvider, valuesResolver);

        InsertFromValuesAnalyzedStatement statement = new InsertFromValuesAnalyzedStatement(
                tableInfo, analysis.parameterContext().hasBulkParams());
        handleInsertColumns(node, node.maxValuesLength(), statement);

        for (ValuesList valuesList : node.valuesLists()) {
            analyzeValues(
                    tableRelation,
                    expressionAnalyzer,
                    expressionAnalysisContext,
                    valuesResolver,
                    valuesAwareExpressionAnalyzer,
                    valuesList,
                    node.onDuplicateKeyAssignments(),
                    statement,
                    analysis.parameterContext());
        }
        return statement;
    }

    private void validateValuesSize(List<Expression> values,
                                    InsertFromValuesAnalyzedStatement statement,
                                    DocTableRelation tableRelation) {
        int numValues = values.size();
        int numInsertColumns = statement.columns().size();
        int numGeneratedColumns = tableRelation.tableInfo().generatedColumns().size();
        boolean firstValues = statement.sourceMaps().isEmpty();

        if (numValues != numInsertColumns) {
            if (firstValues
                || tableRelation.tableInfo().generatedColumns().isEmpty()
                || numValues != numInsertColumns - numGeneratedColumns) {
                // do not fail here when numbers are different if:
                //  * we are not at the first values AND
                //  * we have generated columns in the table AND
                //  * we are only missing the generated columns which will be added
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Invalid number of values: Got %d columns specified but %d values",
                        numInsertColumns-numGeneratedColumns, numValues));
            }

        }
    }

    private void analyzeValues(DocTableRelation tableRelation,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext,
                               ValuesResolver valuesResolver,
                               ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                               ValuesList node,
                               List<Assignment> assignments,
                               InsertFromValuesAnalyzedStatement statement,
                               ParameterContext parameterContext) {
        validateValuesSize(node.values(), statement, tableRelation);

        try {
            DocTableInfo tableInfo = statement.tableInfo();
            int numPks = tableInfo.primaryKey().size();
            Function<List<BytesRef>, String> idFunction = Id.compile(tableInfo.primaryKey(), tableInfo.clusteredBy());
            if (parameterContext.bulkParameters.length > 0) {
                for (int i = 0; i < parameterContext.bulkParameters.length; i++) {
                    parameterContext.setBulkIdx(i);
                    addValues(
                            tableRelation,
                            expressionAnalyzer,
                            expressionAnalysisContext,
                            valuesResolver,
                            valuesAwareExpressionAnalyzer,
                            node,
                            assignments,
                            statement,
                            numPks,
                            idFunction
                    );
                }
            } else {
                addValues(
                        tableRelation,
                        expressionAnalyzer,
                        expressionAnalysisContext,
                        valuesResolver,
                        valuesAwareExpressionAnalyzer,
                        node,
                        assignments,
                        statement,
                        numPks,
                        idFunction
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // can't throw IOException directly because of visitor interface
        }
    }

    private void addValues(DocTableRelation tableRelation,
                           ExpressionAnalyzer expressionAnalyzer,
                           ExpressionAnalysisContext expressionAnalysisContext,
                           ValuesResolver valuesResolver,
                           ExpressionAnalyzer valuesAwareExpressionAnalyzer,
                           ValuesList node,
                           List<Assignment> assignments,
                           InsertFromValuesAnalyzedStatement context,
                           int numPrimaryKeys, Function<List<BytesRef>, String> idFunction) throws IOException {
        if (context.tableInfo().isPartitioned()) {
            context.newPartitionMap();
        }
        List<BytesRef> primaryKeyValues = new ArrayList<>(numPrimaryKeys);
        String routingValue = null;
        List<ColumnIdent> primaryKey = context.tableInfo().primaryKey();
        Object[] insertValues = new Object[node.values().size()];

        for (int i = 0, valuesSize = node.values().size(); i < valuesSize; i++) {
            Expression expression = node.values().get(i);
            Symbol valuesSymbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);

            // implicit type conversion
            Reference column = context.columns().get(i);
            final ColumnIdent columnIdent = column.info().ident().columnIdent();
            Object value;
            try {
                valuesSymbol = expressionAnalyzer.normalizeInputForReference(valuesSymbol, column, expressionAnalysisContext);
                value = ((Input) valuesSymbol).value();
            } catch (IllegalArgumentException | UnsupportedOperationException e) {
                throw new ColumnValidationException(columnIdent.sqlFqn(), e);
            } catch (ClassCastException e) {
                // symbol is no Input
                throw new ColumnValidationException(columnIdent.name(),
                        SymbolFormatter.formatTmpl("Invalid value '%s' in insert statement", valuesSymbol));
            }

            if (context.primaryKeyColumnIndices().contains(i)) {
                if (value == null) {
                    throw new IllegalArgumentException("Primary key value must not be NULL");
                }
                int idx = primaryKey.indexOf(columnIdent);
                if (idx < 0) {
                    // oh look, one or more nested primary keys!
                    assert value instanceof Map;
                    for (ColumnIdent pkIdent : primaryKey) {
                        if (!pkIdent.getRoot().equals(columnIdent)) {
                            continue;
                        }
                        int pkIdx = primaryKey.indexOf(pkIdent);
                        Object nestedValue = StringObjectMaps.fromMapByPath((Map) value, pkIdent.path());
                        addPrimaryKeyValue(pkIdx, nestedValue, primaryKeyValues);
                    }
                } else {
                    addPrimaryKeyValue(idx, value, primaryKeyValues);
                }
            }
            if (i == context.routingColumnIndex()) {
                routingValue = extractRoutingValue(columnIdent, value, context);
            }
            if (context.partitionedByIndices().contains(i)) {
                Object rest = processPartitionedByValues(columnIdent, value, context);
                if (rest != null) {
                    insertValues[i] = rest;
                }
            } else {
                insertValues[i] = value;
            }
        }

        if (!assignments.isEmpty()) {
            valuesResolver.insertValues = insertValues;
            valuesResolver.columns = context.columns();
            Symbol[] onDupKeyAssignments = new Symbol[assignments.size()];
            valuesResolver.assignmentColumns = new ArrayList<>(assignments.size());
            for (int i = 0; i < assignments.size(); i++) {
                Assignment assignment = assignments.get(i);
                Reference columnName = tableRelation.resolveField(
                        (Field) expressionAnalyzer.convert(assignment.columnName(), expressionAnalysisContext));
                assert columnName != null;

                Symbol assignmentExpression = expressionAnalyzer.normalizeInputForReference(
                        valuesAwareExpressionAnalyzer.convert(assignment.expression(), expressionAnalysisContext),
                        columnName,
                        expressionAnalysisContext);
                assignmentExpression = valuesAwareExpressionAnalyzer.normalize(assignmentExpression);
                onDupKeyAssignments[i] = assignmentExpression;

                UpdateStatementAnalyzer.ensureUpdateIsAllowed(
                        tableRelation.tableInfo(), columnName.ident().columnIdent(), onDupKeyAssignments[i]);
                if (valuesResolver.assignmentColumns.size() == i) {
                    valuesResolver.assignmentColumns.add(columnName.ident().columnIdent().fqn());
                }
            }
            context.addOnDuplicateKeyAssignments(onDupKeyAssignments);
            context.addOnDuplicateKeyAssignmentsColumns(
                    valuesResolver.assignmentColumns.toArray(new String[valuesResolver.assignmentColumns.size()]));
        }

        // process generated column expressions and add columns + values
        insertValues = processGeneratedExpressions(tableRelation, context, expressionAnalyzer, insertValues);

        context.sourceMaps().add(insertValues);
        context.addIdAndRouting(idFunction.apply(primaryKeyValues), routingValue);
    }

    private void addPrimaryKeyValue(int index, Object value, List<BytesRef> primaryKeyValues) {
        if (value == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        if (primaryKeyValues.size() > index) {
            primaryKeyValues.add(index, BytesRefs.toBytesRef(value));
        } else {
            primaryKeyValues.add(BytesRefs.toBytesRef(value));
        }
    }

    private String extractRoutingValue(ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        ColumnIdent clusteredByIdent = context.tableInfo().clusteredBy();
        assert clusteredByIdent != null : "clusteredByIdent must not be null";
        if (columnValue != null && !columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert columnValue instanceof Map;
            columnValue = StringObjectMaps.fromMapByPath((Map) columnValue, clusteredByIdent.path());
        }
        if (columnValue == null) {
            throw new IllegalArgumentException("Clustered by value must not be NULL");
        }
        return BytesRefs.toString(columnValue);
    }

    private Object processPartitionedByValues(final ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalyzedStatement context) {
        int idx = context.tableInfo().partitionedBy().indexOf(columnIdent);
        Map<String, String> partitionMap = context.currentPartitionMap();
        if (idx < 0) {
            if (columnValue == null) {
                return null;
            }
            assert columnValue instanceof Map;
            Map<String, Object> mapValue = (Map<String, Object>) columnValue;
            // hmpf, one or more nested partitioned by columns

            for (ColumnIdent partitionIdent : context.tableInfo().partitionedBy()) {
                if (partitionIdent.getRoot().equals(columnIdent)) {
                    Object nestedValue = mapValue.remove(StringUtils.PATH_JOINER.join(partitionIdent.path()));
                    if (nestedValue instanceof BytesRef) {
                        nestedValue = ((BytesRef) nestedValue).utf8ToString();
                    }
                    if (partitionMap != null) {
                        partitionMap.put(partitionIdent.fqn(), BytesRefs.toString(nestedValue));
                    }
                }
            }

            // put the rest into source
            return mapValue;
        } else if (partitionMap != null) {
            partitionMap.put(columnIdent.name(), BytesRefs.toString(columnValue));
        }
        return null;
    }

    private Object[] processGeneratedExpressions(DocTableRelation tableRelation,
                                                 InsertFromValuesAnalyzedStatement context,
                                                 ExpressionAnalyzer expressionAnalyzer,
                                                 Object[] insertValues) {
        ReferenceToLiteralConverter.Context toLiteralContext = new ReferenceToLiteralConverter.Context(context.columns(), insertValues);
        for (GeneratedReferenceInfo referenceInfo : tableRelation.tableInfo().generatedColumns()) {
            Reference reference = new Reference(referenceInfo);
            Symbol valueSymbol = TO_LITERAL_CONVERTER.process(referenceInfo.generatedExpression(), toLiteralContext);
            valueSymbol = expressionAnalyzer.normalize(valueSymbol);
            if (valueSymbol.symbolType() == SymbolType.LITERAL) {
                Object value = ((Input) valueSymbol).value();
                if (tableRelation.tableInfo().isPartitioned()
                    && tableRelation.tableInfo().partitionedByColumns().contains(referenceInfo)) {
                    addGeneratedPartitionedColumnValue(referenceInfo.ident().columnIdent(), value, context.currentPartitionMap());
                } else {
                    insertValues = addGeneratedColumnValue(context, reference, value, insertValues);
                }
            }
        }
        return insertValues;
    }

    private Object[] addGeneratedColumnValue(InsertFromValuesAnalyzedStatement context,
                                             Reference reference,
                                             Object value,
                                             Object[] insertValues) {
        int idx = context.columns().indexOf(reference);
        if (idx == -1) {
            // add column & value
            context.columns().add(reference);
            int valuesIdx = insertValues.length;
            insertValues = Arrays.copyOf(insertValues, insertValues.length + 1);
            insertValues[valuesIdx] = value;
        } else if (insertValues.length <= idx) {
            // only add value
            insertValues = Arrays.copyOf(insertValues, idx + 1);
            insertValues[idx] = value;
        } else if (!insertValues[idx].equals(value)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Given value %s for generated column does not match defined generated expression value %s",
                    insertValues[idx], value));
        }
        return insertValues;
    }

    private void addGeneratedPartitionedColumnValue(ColumnIdent columnIdent,
                                                    Object value,
                                                    Map<String, String> partitionMap) {
        assert partitionMap != null;
        String generatedValue = BytesRefs.toString(value);
        String givenValue = partitionMap.get(columnIdent.name());
        if (givenValue == null) {
            partitionMap.put(columnIdent.name(), generatedValue);
        } else if (!givenValue.equals(generatedValue)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Given value %s for generated column does not match defined generated expression value %s",
                    givenValue, generatedValue));
        }
    }

}
