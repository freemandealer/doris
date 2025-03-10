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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pushdown Alias (inside must be Slot) through Join.
 */
public class PushdownAliasThroughJoin extends OneRewriteRuleFactory {
    private boolean isAllSlotOrAliasSlot(LogicalProject<? extends Plan> project) {
        return project.getProjects().stream().allMatch(expr -> {
            if (expr instanceof Slot) {
                return true;
            }
            if (expr instanceof Alias) {
                return ((Alias) expr).child() instanceof Slot;
            }
            return false;
        });
    }

    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
            .when(this::isAllSlotOrAliasSlot)
            .then(project -> {
                LogicalJoin<? extends Plan, ? extends Plan> join = project.child();
                // aliasMap { Slot -> Alias<Slot> }
                Map<Expression, NamedExpression> aliasMap = project.getProjects().stream()
                        .filter(expr -> expr instanceof Alias && ((Alias) expr).child() instanceof Slot)
                        .map(expr -> (Alias) expr).collect(Collectors.toMap(UnaryNode::child, expr -> expr));
                if (aliasMap.isEmpty()) {
                    return null;
                }
                List<NamedExpression> newProjects = project.getProjects().stream().map(NamedExpression::toSlot)
                        .collect(Collectors.toList());

                List<Slot> leftOutput = join.left().getOutput();
                List<NamedExpression> leftProjects = leftOutput.stream().map(slot -> {
                    NamedExpression alias = aliasMap.get(slot);
                    if (alias != null) {
                        return alias;
                    }
                    return slot;
                }).collect(Collectors.toList());
                List<Slot> rightOutput = join.right().getOutput();
                List<NamedExpression> rightProjects = rightOutput.stream().map(slot -> {
                    NamedExpression alias = aliasMap.get(slot);
                    if (alias != null) {
                        return alias;
                    }
                    return slot;
                }).collect(Collectors.toList());

                Plan left;
                Plan right;
                if (leftOutput.equals(leftProjects)) {
                    left = join.left();
                } else {
                    left = new LogicalProject<>(leftProjects, join.left());
                }
                if (rightOutput.equals(rightProjects)) {
                    right = join.right();
                } else {
                    right = new LogicalProject<>(rightProjects, join.right());
                }

                // If condition use alias slot, we should replace condition
                // project a.id as aid -- join a.id = b.id  =>
                // join aid = b.id -- project a.id as aid
                Map<ExprId, Slot> replaceMap = aliasMap.entrySet().stream().collect(
                        Collectors.toMap(entry -> ((Slot) entry.getKey()).getExprId(),
                                entry -> entry.getValue().toSlot()));

                List<Expression> newHash = replaceJoinConjuncts(join.getHashJoinConjuncts(), replaceMap);
                List<Expression> newOther = replaceJoinConjuncts(join.getOtherJoinConjuncts(), replaceMap);

                Plan newJoin = join.withConjunctsChildren(newHash, newOther, left, right);
                return new LogicalProject<>(newProjects, newJoin);
            }).toRule(RuleType.PUSHDOWN_ALIAS_THROUGH_JOIN);
    }

    private List<Expression> replaceJoinConjuncts(List<Expression> joinConjuncts, Map<ExprId, Slot> replaceMaps) {
        return joinConjuncts.stream().map(expr -> expr.rewriteUp(e -> {
            if (e instanceof Slot && replaceMaps.containsKey(((Slot) e).getExprId())) {
                return replaceMaps.get(((Slot) e).getExprId());
            } else {
                return e;
            }
        })).collect(ImmutableList.toImmutableList());
    }
}
