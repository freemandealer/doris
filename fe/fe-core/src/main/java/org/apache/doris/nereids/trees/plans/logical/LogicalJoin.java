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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Logical join plan.
 */
public class LogicalJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join {

    private final JoinType joinType;
    private final List<Expression> otherJoinConjuncts;
    private final List<Expression> hashJoinConjuncts;
    private final JoinHint hint;

    // When the predicate condition contains subqueries and disjunctions, the join will be marked as MarkJoin.
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();

    public LogicalJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE,
                Optional.empty(), Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE, Optional.empty(),
                Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            JoinHint hint, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, Optional.empty(), Optional.empty(),
                Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts,
                otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    /**
     * Just use in withXXX method.
     */
    private LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            JoinHint hint, Optional<MarkJoinSlotReference> markJoinSlotReference,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
            JoinReorderContext joinReorderContext) {
        super(PlanType.LOGICAL_JOIN, Optional.empty(), Optional.empty(), leftChild, rightChild);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.otherJoinConjuncts = ImmutableList.copyOf(otherJoinConjuncts);
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        this.joinReorderContext.copyFrom(joinReorderContext);
        this.markJoinSlotReference = markJoinSlotReference;
    }

    private LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.otherJoinConjuncts = ImmutableList.copyOf(otherJoinConjuncts);
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        this.markJoinSlotReference = markJoinSlotReference;
    }

    public List<Expression> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    public Set<Slot> getConditionSlot() {
        return Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    public Optional<Expression> getOnClauseCondition() {
        return ExpressionUtils.optionalAnd(hashJoinConjuncts, otherJoinConjuncts);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public JoinHint getHint() {
        return hint;
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }

    @Override
    public List<Slot> computeOutput() {
        return JoinUtils.getJoinOutput(joinType, left(), right());
    }

    @Override
    public List<Slot> computeNonUserVisibleOutput() {
        return markJoinSlotReference.<ImmutableList<Slot>>map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "type", joinType,
                "markJoinSlotReference", markJoinSlotReference,
                "hashJoinConjuncts", hashJoinConjuncts,
                "otherJoinConjuncts", otherJoinConjuncts);
        if (hint != JoinHint.NONE) {
            args.add("hint");
            args.add(hint);
        }
        return Utils.toSqlString("LogicalJoin", args.toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalJoin<?, ?> that = (LogicalJoin<?, ?>) o;
        return joinType == that.joinType
                && hint == that.hint
                && hashJoinConjuncts.equals(that.hashJoinConjuncts)
                && otherJoinConjuncts.equals(that.otherJoinConjuncts)
                && Objects.equals(markJoinSlotReference, that.markJoinSlotReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(otherJoinConjuncts)
                .build();
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    @Override
    public LEFT_CHILD_TYPE left() {
        return (LEFT_CHILD_TYPE) child(0);
    }

    @Override
    public RIGHT_CHILD_TYPE right() {
        return (RIGHT_CHILD_TYPE) child(1);
    }

    @Override
    public LogicalJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, children.get(0),
                children.get(1), joinReorderContext);
    }

    @Override
    public LogicalJoin<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        LogicalJoin<Plan, Plan> newJoin = new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference,
                groupExpression, Optional.of(getLogicalProperties()), left(), right());
        newJoin.getJoinReorderContext().copyFrom(this.getJoinReorderContext());
        return newJoin;
    }

    @Override
    public LogicalJoin<Plan, Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        LogicalJoin<Plan, Plan> newJoin = new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference,
                Optional.empty(), logicalProperties, left(), right());
        newJoin.getJoinReorderContext().copyFrom(this.getJoinReorderContext());
        return newJoin;
    }

    public LogicalJoin<Plan, Plan> withHashJoinConjuncts(List<Expression> hashJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, this.otherJoinConjuncts, hint, markJoinSlotReference,
                left(), right(), joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withJoinConjuncts(
            List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                hint, markJoinSlotReference, left(), right(), joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withHashJoinConjunctsAndChildren(
            List<Expression> hashJoinConjuncts, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, children.get(0),
                children.get(1), joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withConjunctsChildren(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, Plan left, Plan right) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference, left,
                right, joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withJoinType(JoinType joinType) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, left(), right(),
                joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withOtherJoinConjuncts(List<Expression> otherJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, left(), right(),
                joinReorderContext);
    }
}
