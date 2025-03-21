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

suite("test_array_functions") {
    def tableName = "tbl_test_array_functions"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k4` ARRAY<int(11)> NULL COMMENT "",
              `k5` ARRAY<CHAR(5)> NULL COMMENT "",
              `k6` ARRAY<date> NULL COMMENT "",
              `k7` ARRAY<datetime> NULL COMMENT "",
              `k8` ARRAY<datev2> NULL COMMENT "",
              `k9` ARRAY<datev2> NULL COMMENT "",
              `k10` ARRAY<datetimev2(3)> NULL COMMENT "",
              `k11` ARRAY<datetimev2(3)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1,[1,2,3],["a","b",""],[1,2],["hi"],["2015-03-13"],["2015-03-13 12:36:38"],["2023-02-05","2023-02-06"],["2023-02-07","2023-02-06"],['2022-10-15 10:30:00.999', '2022-08-31 12:00:00.999'],['2022-10-16 10:30:00.999', '2022-08-31 12:00:00.999']) """
    sql """ INSERT INTO ${tableName} VALUES(2,[4],NULL,[5],["hi2"],NULL,NULL,["2023-01-05","2023-01-06"],["2023-01-07","2023-01-06"],['2022-11-15 10:30:00.999', '2022-01-31 12:00:00.999'],['2022-11-16 10:30:00.999', '2022-01-31 12:00:00.999']) """
    sql """ INSERT INTO ${tableName} VALUES(3,[],[],NULL,["hi3"],NULL,NULL,NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(4,[1,2,3,4,5,4,3,2,1],[],[],NULL,NULL,NULL,NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(5,[],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(6,[1,2,3,4,5,4,3,2,1],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(7,[8,9,NULL,10,NULL],["f",NULL,"g",NULL,"h"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(8,[1,2,3,3,4,4,NULL],["a","b","b","b"],[1,2,2,3],["hi","hi","hello"],["2015-03-13"],["2015-03-13 12:36:38"],NULL,NULL,NULL,NULL) """
    sql """ INSERT INTO ${tableName} VALUES(9,[1,2,3],["a","b",""],[1,2],["hi"],["2015-03-13","2015-03-13","2015-03-14"],["2015-03-13 12:36:38","2015-03-13 12:36:38"],NULL,NULL,NULL,NULL) """

    qt_select "SELECT k1, size(k2), size(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, cardinality(k2), cardinality(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k2), array_distinct(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k8), array_distinct(k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k2, k1), k1 FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_sort(k2), array_sort(k3), array_sort(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_sort(k8), array_sort(k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k8, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k10, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k2), reverse(k3), reverse(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k8), reverse(k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_join(k2, '_', 'null'), array_join(k3, '-', 'null') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k2, 1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_with_constant(3, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_with_constant(10, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_with_constant(2, 'a') from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_with_constant(2, 123) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(2, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(k1, null, '2020-01-01') from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(null, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k2, 5) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select_array "SELECT k1, array(k1), array_contains(array(k1), k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k2, [1, null, 2], k4, [null]) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k10, k11, array(cast('2023-03-05 10:30:00.999' as datetimev2(3)))) FROM ${tableName} ORDER BY k1"

    def tableName2 = "tbl_test_array_range"
    sql """DROP TABLE IF EXISTS ${tableName2}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
              `k1` int(11) NULL COMMENT "",
              `k2` int(11) NULL COMMENT "",
              `k3` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName2} VALUES(-1,3,5) """
    sql """ INSERT INTO ${tableName2} VALUES(1,3,5) """
    sql """ INSERT INTO ${tableName2} VALUES(2,10,2) """
    sql """ INSERT INTO ${tableName2} VALUES(3,NULL,NULL) """
    sql """ INSERT INTO ${tableName2} VALUES(4,6,1) """
    sql """ INSERT INTO ${tableName2} VALUES(5,10,1) """
    sql """ INSERT INTO ${tableName2} VALUES(6,NULL,1) """
    sql """ INSERT INTO ${tableName2} VALUES(7,10,NULL) """
    sql """ INSERT INTO ${tableName2} VALUES(NULL,10,2) """
    sql """ INSERT INTO ${tableName2} VALUES(8,2,2) """
    sql """ INSERT INTO ${tableName2} VALUES(9,10,6) """

    qt_select "SELECT k1, array_range(k1) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_range(k1,k2) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_range(k1,k2,k3) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k3) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k4) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k7) from ${tableName} ORDER BY k1"

    qt_select "select k2, bitmap_to_string(bitmap_from_array(k2)) from ${tableName} order by k1;"
    
    def tableName3 = "tbl_test_array_set"
    sql """DROP TABLE IF EXISTS ${tableName3}"""
    sql """
            create table IF NOT EXISTS ${tableName3}(
                  class_id int ,
                  class_name varchar(20),
                  student_ids array<int>
                  ) ENGINE=OLAP
                  DUPLICATE KEY(`class_id`,class_name)
                  COMMENT "OLAP"
                  DISTRIBUTED BY HASH(`class_name`) BUCKETS 2
                  PROPERTIES (
                  "replication_allocation" = "tag.location.default: 1",
                  "in_memory" = "false",
                  "storage_format" = "V2"
                  );
        """
    sql """ insert into ${tableName3} values (10005,'aaaaa',[10005,null,null]) """
    sql """ insert into ${tableName3} values (10006,'bbbbb',[60002,60002,60003,null,60005]) """
    
    qt_select_union "select class_id, student_ids, array_union(student_ids,[1,2,3]) from ${tableName3} order by class_id;"
    qt_select_except "select class_id, student_ids, array_except(student_ids,[1,2,3]) from ${tableName3} order by class_id;"
    qt_select_intersect "select class_id, student_ids, array_intersect(student_ids,[1,2,3,null]) from ${tableName3} order by class_id;"

    def tableName4 = "tbl_test_array_datetimev2_functions"

    sql """DROP TABLE IF EXISTS ${tableName4}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName4} (
              `k1` int COMMENT "",
              `k2` ARRAY<datetimev2(3)> COMMENT "",
              `k3` ARRAY<datetimev2(3)> COMMENT "",
              `k4` ARRAY<datetimev2(6)> COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName4} VALUES(1,
                                            ["2023-01-19 18:11:11.1111","2023-01-19 18:22:22.2222","2023-01-19 18:33:33.3333"],
                                            ["2023-01-19 18:22:22.2222","2023-01-19 18:33:33.3333","2023-01-19 18:44:44.4444"],
                                            ["2023-01-19 18:11:11.111111","2023-01-19 18:22:22.222222","2023-01-19 18:33:33.333333"]) """

    qt_select_array_datetimev2_1 "SELECT * FROM ${tableName4}"
    qt_select_array_datetimev2_2 "SELECT if(1,k2,k3) FROM ${tableName4}"
    qt_select_array_datetimev2_3 "SELECT if(0,k2,k3) FROM ${tableName4}"
    qt_select_array_datetimev2_4 "SELECT if(0,k2,k4) FROM ${tableName4}"
}
