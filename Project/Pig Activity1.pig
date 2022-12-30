-- load the input files
inputFile = LOAD 'hdfs:///user/vandana/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- add a filter to filter out the first 2 lines from the input files
ranked = RANK inputFile;
ranked_lines = FILTER ranked BY (rank_inputFile > 1);

-- Group the lines by names
grpd = GROUP ranked_lines BY name;

-- Count the number of lines by each character
total_count = FOREACH grpd GENERATE $0 as name, COUNT($1) as no_of_lines;
result = ORDER total_count BY no_of_lines DESC;

-- remove the output folder before every execution
rmf hdfs:///user/vandana/PigProjectOutput;

-- storing the result
STORE result INTO 'hdfs:///user/vandana/PigProjectOutput';
