-- Load input file from HDFS
inputDialogues = LOAD 'hdfs:///user/root/pigActivity1/*.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Group By name
grp_name = GROUP inputDialogues by name;

-- Count number lines for each name
counts = FOREACH grp_name GENERATE group, COUNT(inputDialogues.line) As cnt;

-- Output in Descending order by the number lines spoken.
counts_order = ORDER counts BY cnt DESC;

-- Store the result in HDFS
STORE counts_order INTO 'hdfs:///user/root/ProjectResults';

