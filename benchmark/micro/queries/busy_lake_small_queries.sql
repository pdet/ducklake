SELECT count(*) FROM lake.events;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_293;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_41;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_248;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_133;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_18;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_0;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_74;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_240;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_191;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1920 THEN error('wrong count') END FROM lake.bg_163;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_11;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_139;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_250;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_101;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_211;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_275;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_276;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_48;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_98;
SELECT CASE WHEN min(id) IS DISTINCT FROM 0 OR max(id) IS DISTINCT FROM 1919 THEN error('wrong range') END
FROM lake.bg_288;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1 THEN error('wrong lookup') END FROM lake.bg_283 WHERE id = 1432;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1 THEN error('wrong lookup') END FROM lake.bg_135 WHERE id = 1357;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1 THEN error('wrong lookup') END FROM lake.bg_45 WHERE id = 1729;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1 THEN error('wrong lookup') END FROM lake.bg_217 WHERE id = 687;
SELECT CASE WHEN count(*) IS DISTINCT FROM 1 THEN error('wrong lookup') END FROM lake.bg_47 WHERE id = 741;
SELECT CASE WHEN min(k) IS DISTINCT FROM 10 OR max(k) IS DISTINCT FROM 99 THEN error('wrong range') END
FROM lake.events;
SELECT CASE WHEN count(*) IS DISTINCT FROM 81920 THEN error('wrong count') END FROM lake.frag_3;
SELECT CASE WHEN max(id) IS DISTINCT FROM 81919 THEN error('wrong max') END FROM lake.frag_7;
SELECT CASE WHEN count(*) IS DISTINCT FROM 900 THEN error('wrong count') END FROM lake.events WHERE id < 1000;
