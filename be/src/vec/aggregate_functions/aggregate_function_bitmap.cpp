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

#include "vec/aggregate_functions/aggregate_function_bitmap.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

template <bool nullable, template <bool, typename> class AggregateFunctionTemplate>
static IAggregateFunction* create_with_int_data_type(const DataTypes& argument_type) {
    auto type = remove_nullable(argument_type[0]);
    WhichDataType which(type);
#define DISPATCH(TYPE)                                                                     \
    if (which.idx == TypeIndex::TYPE) {                                                    \
        return new AggregateFunctionTemplate<nullable, ColumnVector<TYPE>>(argument_type); \
    }
    FOR_INTEGER_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_bitmap_union(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable) {
    return AggregateFunctionPtr(
            creator_without_type::create<AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>(
                    result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_bitmap_intersect(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable) {
    return AggregateFunctionPtr(creator_without_type::create<
                                AggregateFunctionBitmapOp<AggregateFunctionBitmapIntersectOp>>(
            result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_group_bitmap_xor(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable) {
    return AggregateFunctionPtr(creator_without_type::create<
                                AggregateFunctionBitmapOp<AggregateFunctionGroupBitmapXorOp>>(
            result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_bitmap_union_count(const std::string& name,
                                                                  const DataTypes& argument_types,
                                                                  const bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return std::make_shared<AggregateFunctionBitmapCount<true, ColumnBitmap>>(argument_types);
    } else {
        return std::make_shared<AggregateFunctionBitmapCount<false, ColumnBitmap>>(argument_types);
    }
}

AggregateFunctionPtr create_aggregate_function_bitmap_union_int(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    if (arg_is_nullable) {
        return std::shared_ptr<IAggregateFunction>(
                create_with_int_data_type<true, AggregateFunctionBitmapCount>(argument_types));
    } else {
        return std::shared_ptr<IAggregateFunction>(
                create_with_int_data_type<false, AggregateFunctionBitmapCount>(argument_types));
    }
}

void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("bitmap_union", create_aggregate_function_bitmap_union);
    factory.register_function_both("bitmap_intersect", create_aggregate_function_bitmap_intersect);
    factory.register_function_both("group_bitmap_xor", create_aggregate_function_group_bitmap_xor);
    factory.register_function_both("bitmap_union_count",
                                   create_aggregate_function_bitmap_union_count);
    factory.register_function_both("bitmap_union_int", create_aggregate_function_bitmap_union_int);
}
} // namespace doris::vectorized