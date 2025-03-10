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

#pragma once

#include "vec/exec/vset_operation_node.h"

namespace doris {
namespace vectorized {

class VUnionNode final : public ExecNode {
public:
    VUnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status materialize_child_block(RuntimeState* state, int child_id,
                                   vectorized::Block* input_block, vectorized::Block* output_block);

    size_t children_count() const { return _children.size(); }

    int get_first_materialized_child_idx() const { return _first_materialized_child_idx; }

    /// Returns true if there are still rows to be returned from constant expressions.
    bool has_more_const(const RuntimeState* state) const {
        return state->per_fragment_instance_idx() == 0 &&
               _const_expr_list_idx < _const_expr_lists.size();
    }

    /// GetNext() for the constant expression case.
    Status get_next_const(RuntimeState* state, Block* block);

private:
    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    std::vector<std::vector<VExprContext*>> _const_expr_lists;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;
    /// Index of the first non-passthrough child; i.e. a child that needs materialization.
    /// 0 when all children are materialized, '_children.size()' when no children are
    /// materialized.
    const int _first_materialized_child_idx;
    /// Index of current const result expr list.
    int _const_expr_list_idx;

    /// Index of current child.
    int _child_idx;

    /// Index of current row in child_row_block_.
    int _child_row_idx;

    /// Saved from the last to GetNext() on the current child.
    bool _child_eos;

    /// Index of the child that needs to be closed on the next GetNext() call. Should be set
    /// to -1 if no child needs to be closed.
    int _to_close_child_idx;

    // Time spent to evaluates exprs and materializes the results
    RuntimeProfile::Counter* _materialize_exprs_evaluate_timer = nullptr;
    /// GetNext() for the passthrough case. We pass 'block' directly into the GetNext()
    /// call on the child.
    Status get_next_pass_through(RuntimeState* state, Block* block);

    /// GetNext() for the materialized case. Materializes and evaluates rows from each
    /// non-passthrough child.
    Status get_next_materialized(RuntimeState* state, Block* block);

    /// Evaluates exprs for the current child and materializes the results into 'tuple_buf',
    /// which is attached to 'dst_block'. Runs until 'dst_block' is at capacity, or all rows
    /// have been consumed from the current child block. Updates '_child_row_idx'.
    Block materialize_block(Block* dst_block, int child_idx);

    Status get_error_msg(const std::vector<VExprContext*>& exprs);

    /// Returns true if the child at 'child_idx' can be passed through.
    bool is_child_passthrough(int child_idx) const {
        DCHECK_LT(child_idx, _children.size());
        return child_idx < _first_materialized_child_idx;
    }

    /// Returns true if there are still rows to be returned from passthrough children.
    bool has_more_passthrough() const { return _child_idx < _first_materialized_child_idx; }

    /// Returns true if there are still rows to be returned from children that need
    /// materialization.
    bool has_more_materialized() const {
        return _first_materialized_child_idx != _children.size() && _child_idx < _children.size();
    }

    void debug_string(int indentation_level, std::stringstream* out) const override;
};

} // namespace vectorized
} // namespace doris
