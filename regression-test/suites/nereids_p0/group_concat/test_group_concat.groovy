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

suite("test_group_concat") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_select """
                SELECT group_concat(k6) FROM test_query_db.test where k6='false'
              """

    qt_select """
                SELECT group_concat(DISTINCT k6) FROM test_query_db.test where k6='false'
              """

    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar) order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """
              
    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar), ":" order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """

    test {
        sql"""SELECT abs(k3), group_concat(distinct cast(abs(k2) as char) order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);"""
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql"""SELECT abs(k3), group_concat(distinct cast(abs(k2) as char), ":" order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);"""
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    qt_select """
                SELECT count(distinct k7), group_concat(k6 order by k6) FROM test_query_db.baseall;
              """

    sql """drop table if exists table_group_concat;"""

    sql """create table table_group_concat ( b1 varchar(10) not null, b2 int not null, b3 varchar(10) not null )
            ENGINE=OLAP
            DISTRIBUTED BY HASH(b3) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql """insert into table_group_concat values('1', 1, '2'),('1', 1, '2'),('1', 2, '2');"""

    qt_select_10 """
                select
                group_concat( distinct b1 ), group_concat( distinct b3 )
                from
                table_group_concat;
              """

    qt_select_11 """
                select
                group_concat( distinct b1 ), group_concat( distinct b3 )
                from
                table_group_concat
                group by 
                b2;
              """
}
