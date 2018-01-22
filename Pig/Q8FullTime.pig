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


--Year2011 FULL TIME

d2011 = FILTER cleaning1 BY year == '2011' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy1 = FOREACH d2011 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped1 = GROUP dummy1 BY ($0, $1);

avg_pw1 = FOREACH grouped1 GENERATE group, AVG(dummy1.$2) AS AVERAGE_WAGE;

ordering1 = ORDER avg_pw1 BY AVERAGE_WAGE DESC;
print2011 = Limit ordering1 5;

--dump ordering1;

--Year2012  FULL TIME

d2012 = FILTER cleaning1 BY year == '2012' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy2 = FOREACH d2012 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped2 = GROUP dummy2 BY ($0, $1);

avg_pw2 = FOREACH grouped2 GENERATE group, AVG(dummy2.$2) AS AVERAGE_WAGE;

ordering2 = ORDER avg_pw2 BY AVERAGE_WAGE DESC;
print2012 = Limit ordering2 5;


--Year2013  FULL TIME
d2013 = FILTER cleaning1 BY year == '2013' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy3 = FOREACH d2013 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped3 = GROUP dummy3 BY ($0, $1);

avg_pw3 = FOREACH grouped3 GENERATE group, AVG(dummy3.$2) AS AVERAGE_WAGE;

ordering3 = ORDER avg_pw3 BY AVERAGE_WAGE DESC;
print2013 = Limit ordering3 5;


--Year2014  FULL TIME
d2014 = FILTER cleaning1 BY year == '2014' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy4 = FOREACH d2014 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped4 = GROUP dummy4 BY ($0, $1);

avg_pw4 = FOREACH grouped4 GENERATE group, AVG(dummy4.$2) AS AVERAGE_WAGE;

ordering4 = ORDER avg_pw4 BY AVERAGE_WAGE DESC;
print2014 = Limit ordering4 5;

--Year2015  FULL TIME

d2015 = FILTER cleaning1 BY year == '2015' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy5 = FOREACH d2015 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped5 = GROUP dummy5 BY ($0, $1);

avg_pw5 = FOREACH grouped5 GENERATE group, AVG(dummy5.$2) AS AVERAGE_WAGE;

ordering5 = ORDER avg_pw5 BY AVERAGE_WAGE DESC;
print2015 = Limit ordering5 5;

--Year2016 FULL TIME
d2016 = FILTER cleaning1 BY year == '2016' AND full_time_position == 'Y' AND (case_status == 'CERTIFIED' OR case_status == 
'CERTIFIED-WITHDRAWN');

dummy6 = FOREACH d2016 GENERATE $7, $4, $6; /*YEAR, JOB_TITLE, WAGE*/

grouped6 = GROUP dummy6 BY ($0, $1);

avg_pw6 = FOREACH grouped6 GENERATE group, AVG(dummy6.$2) AS AVERAGE_WAGE;

ordering6 = ORDER avg_pw6 BY AVERAGE_WAGE DESC;
print2016 = Limit ordering6 5;

avg_pw_All_Year_Full_Time = UNION print2011, print2012, print2013, print2014, print2015, print2016;
describe avg_pw_All_Year_Full_Time; 

result = Order avg_pw_All_Year_Full_Time by $0 desc;
DUMP result;



