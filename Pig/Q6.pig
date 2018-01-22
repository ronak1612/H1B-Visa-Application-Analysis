

/*

6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.

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


--cleandata = FILTER data BY year =='NA'   0 records;

cleandata = FILTER data BY year!='NA';
--DUMP cleandata;

requiredFields = FOREACH cleandata GENERATE $7, $1;
--year, case_status


onlyCertified = FILTER requiredFields BY $1 == 'CERTIFIED' ;
grouping1 = GROUP onlyCertified BY $0;
certified_counting = FOREACH grouping1 GENERATE group, COUNT(onlyCertified.$1) AS CERTIFIED_COUNT;
--DUMP certified_counting1;
/*
(2011,307936)
(2012,352668)
(2013,382951)
(2014,455144)
(2015,547278)
(2016,569646)
*/

onlyCertifiedWithdrawn = FILTER requiredFields BY $1 == 'CERTIFIED-WITHDRAWN' ;
grouping2 = GROUP onlyCertifiedWithdrawn BY $0;
certified_withdrawn_counting = FOREACH grouping2 GENERATE group, COUNT(onlyCertifiedWithdrawn.$1) AS CertifiedWithdrawn_COUNT;
--DUMP certified_withdrawn_counting;
/*
(2011,11596)
(2012,31118)
(2013,35432)
(2014,36350)
(2015,41071)
(2016,47092)
*/

onlyWithdrawn = FILTER requiredFields BY $1 == 'WITHDRAWN';
grouping3 = GROUP onlyWithdrawn BY $0;
withdrawn_counting = FOREACH grouping3 GENERATE group, COUNT(onlyWithdrawn.$1) AS Withdrawn_COUNT;
--DUMP withdrawn_counting;
/*
(2011,10105)
(2012,10725)
(2013,11590)
(2014,16034)
(2015,19455)
(2016,21890)
*/

onlyDenied = FILTER requiredFields BY $1 == 'DENIED';
grouping4 = GROUP onlyDenied BY $0;
denied_counting = FOREACH grouping4 GENERATE group, COUNT(onlyDenied.$1) AS Denied_COUNT;
--DUMP denied_counting;
/*
(2011,29130)
(2012,21096)
(2013,12141)
(2014,11899)
(2015,10923)
(2016,9175)
*/


--total count of case_status for each year

grouping5 = GROUP requiredFields BY $0; /*group by year*/
total_counting = FOREACH grouping5 GENERATE group, COUNT(requiredFields.$1) AS TOTAL_COUNT; /* counting case_status */
--DUMP total_counting;
/*
(2011,358767)   307936+11596+10105+29130= 358767
(2012,415607)
(2013,442114)
(2014,519427)
(2015,618727)
(2016,647803) */


--Joining Year, Certified_Count, Year, CertifiedWithdrawn_Count, Year, Withdrawn_Count, Year, Denied_Count, Year, total_counting.
--	  $0,		$1,	 $2,	$3,			 $4,	$5,		$6,	$7,	     $8,	$9

joinedData = JOIN certified_counting BY $0, certified_withdrawn_counting BY $0 ,withdrawn_counting BY $0, denied_counting BY $0, total_counting BY $0;

Calculation_Table = FOREACH joinedData GENERATE $0, $1, $3, $5, $7, $9;
--DESCRIBE Calculation_Table;
/*Calculation_Table: { certified_counting::group: chararray,certified_counting::CERTIFIED_COUNT: long,
		      certified_withdrawn_counting::CertifiedWithdrawn_COUNT: long, 
		      withdrawn_counting::Withdrawn_COUNT: long,
		      denied_counting::Denied_COUNT: long,total_counting::TOTAL_COUNT: long}*/

--DUMP Calculation_Table;

/*
 $0,   $1 C, $2 CW, $3 W, $4 D  $5 T
(2011,307936,11596,10105,29130,358767)
(2012,352668,31118,10725,21096,415607)
(2013,382951,35432,11590,12141,442114)
(2014,455144,36350,16034,11899,519427)
(2015,547278,41071,19455,10923,618727)
(2016,569646,47092,21890,9175,647803)
*/

Percentage_Calculation = FOREACH Calculation_Table GENERATE $0, $1, $2, $3, $4, $5, ROUND_TO(((DOUBLE)$1*100/$5),2) AS CERTIFIED_PERCENTAGE, 
ROUND_TO(((DOUBLE)$2*100/$5),2) AS CERTIFIED_WITHDRAWN_PERCENTAGE, ROUND_TO(((DOUBLE)$3*100/$5),2) AS WITHDRAWN_PERCENTAGE, ROUND_TO(((DOUBLE)$4*100/$5),2) AS DENIED_PERCENTAGE ;
--DESCRIBE Percentage_Calculation;
DUMP Percentage_Calculation;



