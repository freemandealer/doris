-- ERROR: correlationFilter can't be null in correlatedToJoin
-- SELECT
--   CAST(S.var["S_NAME"] AS TEXT),
--   CAST(S.var["S_ADDRESS"] AS TEXT)
-- FROM
--   supplier S, nation N
-- WHERE
--   CAST(S.var["S_SUPPKEY"] AS INT) IN (
--     SELECT CAST(PS.var["PS_SUPPKEY"] AS INT)
--     FROM
--       partsupp PS
--     WHERE
--       CAST(PS.var["PS_PARTKEY"] AS INT) IN (
--         SELECT CAST(P.var["P_PARTKEY"] AS INT)
--         FROM
--           part P
--         WHERE
--           CAST(P.var["P_NAME"] AS TEXT) LIKE 'FOREST%'
--       )
--       AND CAST(PS.var["PS_AVAILQTY"] AS INT) > (
--         SELECT 0.5 * SUM(CAST(L.var["L_QUANTITY"] AS DOUBLE))
--         FROM
--           lineitem L
--         WHERE
--           CAST(L.var["L_PARTKEY"] AS INT) = CAST(PS.var["PS_PARTKEY"] AS INT)
--           AND CAST(L.var["L_SUPPKEY"] AS INT) = CAST(PS.var["PS_SUPPKEY"] AS INT)
--           AND CAST(L.var["L_SHIPDATE"] AS DATE) >= DATE('1994-01-01')
--           AND CAST(L.var["L_SHIPDATE"] AS DATE) < DATE('1994-01-01') + INTERVAL '1' YEAR
-- )
-- )
-- AND CAST(S.var["S_NATIONKEY"] AS INT) = CAST(N.var["N_NATIONKEY"] AS INT)
-- AND CAST(N.var["N_NAME"] AS TEXT) = 'CANADA'
-- ORDER BY CAST(S.var["S_NAME"] AS TEXT)