// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'convert_tz'. This class is generated by GenerateFunction.
 */
public class ConvertTz extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral, Monotonic {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT)
                    .args(DateTimeV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(DateTimeType.INSTANCE)
                    .args(DateTimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 3 arguments.
     */
    public ConvertTz(Expression arg0, Expression arg1, Expression arg2) {
        super("convert_tz", castDateTime(arg0), arg1, arg2);
    }

    private static Expression castDateTime(Expression arg0) {
        // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_convert-tz
        // convert_tz() should be explicit cast, so we create a explicit cast here
        try {
            return arg0 instanceof StringLikeLiteral ? new Cast(arg0, DateTimeV2Type.forTypeFromString(
                    ((StringLikeLiteral) arg0).getStringValue()), true) : arg0;
        } catch (Exception e) {
            return new NullLiteral(DateTimeV2Type.SYSTEM_DEFAULT);
        }
    }

    /**
     * withChildren.
     */
    @Override
    public ConvertTz withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new ConvertTz(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitConvertTz(this, context);
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Literal literal) {
        return new ConvertTz(literal, child(1), child(2));
    }
}
