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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * simplify comparison
 * such as: cast(c1 as DateV2) >= DateV2Literal --> c1 >= DateLiteral
 *          cast(c1 AS double) > 2.0 --> c1 >= 2 (c1 is integer like type)
 */
public class SimplifyComparisonPredicate extends AbstractExpressionRewriteRule implements ExpressionPatternRuleFactory {
    public static SimplifyComparisonPredicate INSTANCE = new SimplifyComparisonPredicate();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class).then(SimplifyComparisonPredicate::simplify)
        );
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        return simplify(cp);
    }

    /** simplify */
    public static Expression simplify(ComparisonPredicate cp) {
        if (cp.left() instanceof Literal && !(cp.right() instanceof Literal)) {
            cp = cp.commute();
        }

        Expression left = cp.left();
        Expression right = cp.right();

        // float like type: float, double
        if (left.getDataType().isFloatLikeType() && right.getDataType().isFloatLikeType()) {
            return processFloatLikeTypeCoercion(cp, left, right);
        }

        // decimalv3 type
        if (left.getDataType() instanceof DecimalV3Type && right.getDataType() instanceof DecimalV3Type) {
            return processDecimalV3TypeCoercion(cp, left, right);
        }

        // date like type
        if (left.getDataType() instanceof DateLikeType && right.getDataType() instanceof DateLikeType) {
            return processDateLikeTypeCoercion(cp, left, right);
        }

        return cp;
    }

    private static Expression processDateTimeLikeComparisonPredicateDateTimeV2Literal(
            ComparisonPredicate comparisonPredicate, Expression left, DateTimeV2Literal right) {
        DataType leftType = left.getDataType();
        int toScale = 0;
        if (leftType instanceof DateTimeType) {
            toScale = 0;
        } else if (leftType instanceof DateTimeV2Type) {
            toScale = ((DateTimeV2Type) leftType).getScale();
        } else {
            return comparisonPredicate;
        }
        DateTimeV2Type rightType = right.getDataType();
        if (toScale < rightType.getScale()) {
            if (comparisonPredicate instanceof EqualTo) {
                long originValue = right.getMicroSecond();
                right = right.roundCeiling(toScale);
                if (right.getMicroSecond() != originValue) {
                    // TODO: the ideal way is to return an If expr like:
                    // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                    // BooleanLiteral.of(false));
                    // but current fold constant rule can't handle such complex expr with null literal
                    // before supporting complex conjuncts with null literal folding rules,
                    // we use a trick way like this:
                    return ExpressionUtils.falseOrNull(left);
                }
            } else if (comparisonPredicate instanceof NullSafeEqual) {
                long originValue = right.getMicroSecond();
                right = right.roundCeiling(toScale);
                if (right.getMicroSecond() != originValue) {
                    return BooleanLiteral.of(false);
                }
            } else if (comparisonPredicate instanceof GreaterThan
                    || comparisonPredicate instanceof LessThanEqual) {
                right = right.roundFloor(toScale);
            } else if (comparisonPredicate instanceof LessThan
                    || comparisonPredicate instanceof GreaterThanEqual) {
                right = right.roundCeiling(toScale);
            } else {
                return comparisonPredicate;
            }
            Expression newRight = leftType instanceof DateTimeType ? migrateToDateTime(right) : right;
            return comparisonPredicate.withChildren(left, newRight);
        } else {
            if (leftType instanceof DateTimeType) {
                return comparisonPredicate.withChildren(left, migrateToDateTime(right));
            } else {
                return comparisonPredicate;
            }
        }
    }

    private static Expression processDateLikeTypeCoercion(ComparisonPredicate cp, Expression left, Expression right) {
        if (left instanceof Cast && right instanceof DateLiteral) {
            Cast cast = (Cast) left;
            if (cast.child().getDataType() instanceof DateTimeType
                    || cast.child().getDataType() instanceof DateTimeV2Type) {
                if (right instanceof DateTimeV2Literal) {
                    return processDateTimeLikeComparisonPredicateDateTimeV2Literal(
                            cp, cast.child(), (DateTimeV2Literal) right);
                }
            }

            // datetime to datev2
            if (cast.child().getDataType() instanceof DateType || cast.child().getDataType() instanceof DateV2Type) {
                if (right instanceof DateTimeLiteral) {
                    DateTimeLiteral dateTimeLiteral = (DateTimeLiteral) right;
                    right = migrateToDateV2(dateTimeLiteral);
                    if (dateTimeLiteral.getHour() != 0 || dateTimeLiteral.getMinute() != 0
                            || dateTimeLiteral.getSecond() != 0 || dateTimeLiteral.getMicroSecond() != 0) {
                        if (cp instanceof EqualTo) {
                            return ExpressionUtils.falseOrNull(cast.child());
                        } else if (cp instanceof NullSafeEqual) {
                            return BooleanLiteral.FALSE;
                        } else if (cp instanceof GreaterThanEqual || cp instanceof LessThan) {
                            right = ((DateV2Literal) right).plusDays(1);
                        }
                    }
                    if (cast.child().getDataType() instanceof DateV2Type) {
                        left = cast.child();
                    }
                }
            }

            // datev2 to date
            if (cast.child().getDataType() instanceof DateType) {
                if (right instanceof DateV2Literal) {
                    left = cast.child();
                    right = migrateToDate((DateV2Literal) right);
                }
            }
        }

        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    private static Expression processFloatLikeTypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof Cast && left.child(0).getDataType().isIntegerLikeType()
                && (right instanceof DoubleLiteral || right instanceof FloatLiteral)) {
            Cast cast = (Cast) left;
            left = cast.child();
            BigDecimal literal = new BigDecimal(((Literal) right).getStringValue());
            return processIntegerDecimalLiteralComparison(comparisonPredicate, left, literal);
        } else {
            return comparisonPredicate;
        }
    }

    private static Expression processDecimalV3TypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof Cast && right instanceof DecimalV3Literal) {
            Cast cast = (Cast) left;
            left = cast.child();
            DecimalV3Literal literal = (DecimalV3Literal) right;
            if (left.getDataType().isDecimalV3Type()) {
                DecimalV3Type leftType = (DecimalV3Type) left.getDataType();
                DecimalV3Type literalType = (DecimalV3Type) literal.getDataType();
                if (leftType.getScale() < literalType.getScale()) {
                    int toScale = ((DecimalV3Type) left.getDataType()).getScale();
                    if (comparisonPredicate instanceof EqualTo) {
                        try {
                            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                    comparisonPredicate.withChildren(left, new DecimalV3Literal(
                                            literal.getValue().setScale(toScale, RoundingMode.UNNECESSARY))));
                        } catch (ArithmeticException e) {
                            // TODO: the ideal way is to return an If expr like:
                            // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                            // BooleanLiteral.of(false));
                            // but current fold constant rule can't handle such complex expr with null literal
                            // before supporting complex conjuncts with null literal folding rules,
                            // we use a trick way like this:
                            return ExpressionUtils.falseOrNull(left);
                        }
                    } else if (comparisonPredicate instanceof NullSafeEqual) {
                        try {
                            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                    comparisonPredicate.withChildren(left, new DecimalV3Literal(
                                            literal.getValue().setScale(toScale, RoundingMode.UNNECESSARY))));
                        } catch (ArithmeticException e) {
                            return BooleanLiteral.of(false);
                        }
                    } else if (comparisonPredicate instanceof GreaterThan
                            || comparisonPredicate instanceof LessThanEqual) {
                        return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                comparisonPredicate.withChildren(left, literal.roundFloor(toScale)));
                    } else if (comparisonPredicate instanceof LessThan
                            || comparisonPredicate instanceof GreaterThanEqual) {
                        return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                comparisonPredicate.withChildren(left, literal.roundCeiling(toScale)));
                    }
                }
            } else if (left.getDataType().isIntegerLikeType()) {
                return processIntegerDecimalLiteralComparison(comparisonPredicate, left, literal.getValue());
            }
        }

        return comparisonPredicate;
    }

    private static Expression processIntegerDecimalLiteralComparison(
            ComparisonPredicate comparisonPredicate, Expression left, BigDecimal literal) {
        // we only process isIntegerLikeType, which are tinyint, smallint, int, bigint
        if (literal.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0
                && literal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0) {
            literal = literal.stripTrailingZeros();
            if (literal.scale() > 0) {
                if (comparisonPredicate instanceof EqualTo) {
                    // TODO: the ideal way is to return an If expr like:
                    // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                    // BooleanLiteral.of(false));
                    // but current fold constant rule can't handle such complex expr with null literal
                    // before supporting complex conjuncts with null literal folding rules,
                    // we use a trick way like this:
                    return ExpressionUtils.falseOrNull(left);
                } else if (comparisonPredicate instanceof NullSafeEqual) {
                    return BooleanLiteral.of(false);
                } else if (comparisonPredicate instanceof GreaterThan
                        || comparisonPredicate instanceof LessThanEqual) {
                    return TypeCoercionUtils
                            .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                    .withChildren(left, convertDecimalToIntegerLikeLiteral(
                                            literal.setScale(0, RoundingMode.FLOOR))));
                } else if (comparisonPredicate instanceof LessThan
                        || comparisonPredicate instanceof GreaterThanEqual) {
                    return TypeCoercionUtils
                            .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                    .withChildren(left, convertDecimalToIntegerLikeLiteral(
                                            literal.setScale(0, RoundingMode.CEILING))));
                }
            } else {
                return TypeCoercionUtils
                        .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                .withChildren(left, convertDecimalToIntegerLikeLiteral(literal)));
            }
        }
        return comparisonPredicate;
    }

    private static IntegerLikeLiteral convertDecimalToIntegerLikeLiteral(BigDecimal decimal) {
        Preconditions.checkArgument(decimal.scale() <= 0
                && decimal.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0
                && decimal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0,
                "decimal literal must have 0 scale and in range [Long.MIN_VALUE, Long.MAX_VALUE]");
        long val = decimal.longValue();
        if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
            return new TinyIntLiteral((byte) val);
        } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
            return new SmallIntLiteral((short) val);
        } else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
            return new IntegerLiteral((int) val);
        } else {
            return new BigIntLiteral(val);
        }
    }

    private static Expression migrateToDateTime(DateTimeV2Literal l) {
        return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), l.getHour(), l.getMinute(), l.getSecond());
    }

    private static Expression migrateToDateV2(DateTimeLiteral l) {
        return new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
    }

    private static Expression migrateToDate(DateV2Literal l) {
        return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
    }
}
