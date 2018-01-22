ds1 = LOAD '/home/hduser/Part1' USING PigStorage('\t') AS  (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

ds2 = LOAD '/home/hduser/Part2.csv' USING PigStorage('\t') AS (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

data = UNION ds1, ds2;


-- removing data fieled with job_title, year and prevailing wage ="" or "NA"

cleaning1 = FILTER data BY year!='NA' AND job_title!='NA' AND full_time_position!='NA';


--Year2011 FULL TIME

d2011 = FILTER cleaning1 BY year == '$year' AND full_time_position == '$position' AND (case_status == 'CERTIFIED' OR 
case_status == 'CERTIFIED-WITHDRAWN');

dummy1 = FOREACH d2011 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped1 = GROUP dummy1 BY ($0, $1);

avg_pw1 = FOREACH grouped1 GENERATE group, AVG(dummy1.$2) AS AVERAGE_WAGE;

ordering1 = ORDER avg_pw1 BY AVERAGE_WAGE DESC;
print2011 = Limit ordering1 5;

Dump print2011;

