
--Q8.Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]

/*
$0	s_no:int,
$1	case_status:chararray,
$2	employer_name:chararray,
$3	soc_name:chararray,
$4	job_title:chararray,
$5	full_time_position:chararray,
$6	prevailing_wage:int,
$7	year:chararray,
$8	worksite:chararray,
$9	longitute:double,
$10	latitute:double); */




ds1 = LOAD '/home/hduser/Part1' USING PigStorage('\t') AS  (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

ds2 = LOAD '/home/hduser/Part2.csv' USING PigStorage('\t') AS (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

data = UNION ds1, ds2;


-- removing data fieled with job_title, year and prevailing wage ="" or "NA"

cleaning1 = FILTER data BY year!='NA' AND job_title!='NA' AND full_time_position!='NA';

--Year2011 PART TIME

d2011P = FILTER cleaning1 BY year == '2011' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy1P = FOREACH d2011P GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped1P = GROUP dummy1P BY ($0, $1);

avg_pw1P = FOREACH grouped1P GENERATE group, AVG(dummy1P.$2) AS AVERAGE_WAGE;

ordering1P = ORDER avg_pw1P BY AVERAGE_WAGE DESC;
print2011 = Limit ordering1P 5;


--Year2012 PART TIME

d2012P = FILTER cleaning1 BY year == '2012' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy2P = FOREACH d2012P GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped2P = GROUP dummy2P BY ($0, $1);

avg_pw2P = FOREACH grouped2P GENERATE group, AVG(dummy2P.$2) AS AVERAGE_WAGE;

ordering2P = ORDER avg_pw2P BY AVERAGE_WAGE DESC;
print2012 = Limit ordering2P 5;

--Year2013 PART TIME

d2013P = FILTER cleaning1 BY year == '2013' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy3P = FOREACH d2013P GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped3P = GROUP dummy3P BY ($0, $1);

avg_pw3P = FOREACH grouped3P GENERATE group, AVG(dummy3P.$2) AS AVERAGE_WAGE;

ordering3P = ORDER avg_pw3P BY AVERAGE_WAGE DESC;

print2013 = Limit ordering3P 5;
--Year2014 PART TIME

d2014P = FILTER cleaning1 BY year == '2014' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy4p = FOREACH d2014P GENERATE $7, $4, $6;

grouped4p = GROUP dummy4p BY ($0, $1);

avg_pw4p = FOREACH grouped4p GENERATE group, dummy4p.$0, dummy4p.$1,  AVG(dummy4p.$2) AS AVERAGE_WAGE;

ordering4p = ORDER avg_pw4p BY $1 ASC, $3 DESC;
print2014 = Limit ordering4p 5;

--Year2015 PART TIME

d2015P = FILTER cleaning1 BY year == '2015' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy5p = FOREACH d2015P GENERATE $7, $4, $6;

grouped5p = GROUP dummy5p BY ($0, $1);

avg_pw5p = FOREACH grouped5p GENERATE group, dummy5p.$0, dummy5p.$1,  AVG(dummy5p.$2) AS AVERAGE_WAGE;

ordering5p = ORDER avg_pw5p BY $1 ASC, $3 DESC;
print2015 = Limit ordering5p 5;
--Year2016 PART TIME

d2016P = FILTER cleaning1 BY year == '2016' AND full_time_position == 'N' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy6p = FOREACH d2016P GENERATE $7, $4, $6;

grouped6p = GROUP dummy6p BY ($0, $1);

avg_pw6p = FOREACH grouped6p GENERATE group, dummy6p.$0, dummy6p.$1,  AVG(dummy6p.$2) AS AVERAGE_WAGE;

ordering6p = ORDER avg_pw6p BY $1 ASC, $3 DESC;

print2016 = Limit ordering6p 5;


avg_pw_All_Year_Part_Time = UNION print2011, print2012, print2013, print2014, print2015, print2016;
result = Order avg_pw_All_Year_Part_Time by $0 desc;
DUMP result;


