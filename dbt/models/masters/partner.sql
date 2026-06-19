-- 마스터: 파트너 100 (스냅샷)
SELECT g AS partner_id,
       CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END AS name,
       CASE WHEN g<=50 THEN 1 ELSE 2 END AS brand
FROM range(1,101) AS t(g)
