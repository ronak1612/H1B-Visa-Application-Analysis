--JobPositionsSuccessRate.pig


/*Goal:  job_title, Count(Certified), Count(Certified-Withdrawn), SuccessRate((C+CW)/total)*100

Step1: Setting up main cleaned dataset
Step2:Counting number of Certified application/petitions for each job_title.
Step3:Counting number of Certified-Withdrawn application/petitions for each job_title.
Step4: Finding Total applications/petitions for sucessrate formula.
Step5: Calculating SuccessRate% For each job title.
Step6: Filtering job_title having SucessRate % more tyhan 70%. 


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

--Step1: Setting up main cleaned dataset.Removing dataset records which has case_status and job_title as 'NA' or "".


ds1 = LOAD '/home/hduser/Part1' USING PigStorage('\t') AS 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);

ds2 = LOAD '/home/hduser/Part2.csv' USING PigStorage('\t') AS 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);

data = UNION ds1, ds2;

--Keeping only records which does not have job_title='NA'
cleandata = FILTER data BY employer_name!='NA';
--DESCRIBE cleandata;


finaldata = FOREACH cleandata GENERATE LOWER($2), $1;
DESCRIBE finaldata;
--finaldata: {org.apache.pig.builtin.lower_employer_name_4: chararray,case_status: chararray}

--PRIN = LIMIT finaldata 5;
--DUMP PRIN;



--STEP2: Counting number of Certified application/petitions for each job_title.

dummy1 = FILTER finaldata BY case_status == 'CERTIFIED';
--DUMP dummy1;

groupdummy1 = GROUP dummy1 BY $0;
--DUMP groupdummy1;

certified = FOREACH groupdummy1 GENERATE group, COUNT(dummy1.case_status) AS CERTIFIED;

--DESCRIBE certified;
--certified: {group: chararray,long}

--DUMP certified;




dummy2 = FILTER finaldata BY case_status == 'CERTIFIED-WITHDRAWN';
--DUMP dummy2;

groupdummy2 = GROUP dummy2 BY $0 ;
certified_Withdrawn = FOREACH groupdummy2 GENERATE group, COUNT(dummy2.case_status) AS CERTIFIED_WITHDRAWN;
--DUMP certified_Withdrawn;


dummy3 = GROUP finaldata BY $0;
totalPetitionPerEmployerName = FOREACH dummy3 GENERATE group, COUNT(finaldata.case_status) AS TOTAL;

DESCRIBE totalPetitionPerEmployerName;




joinedPetition = JOIN certified BY $0, certified_Withdrawn BY $0, totalPetitionPerEmployerName BY $0;

DESCRIBE joinedPetition;


CalculationTable = FOREACH joinedPetition GENERATE $0, $1, $3, $5;
DESCRIBE CalculationTable;


Success_Rate = FOREACH CalculationTable GENERATE $0, ((($1+$2)*100/$3)) AS SUCCESS_RATE_PERCENTAGE, $3 ;
						

--DESCRIBE Success_Rate;


ordering = ORDER Success_Rate BY SUCCESS_RATE_PERCENTAGE DESC;

--DUMP ordering;

result = FILTER ordering BY SUCCESS_RATE_PERCENTAGE>70 and TOTAL>=1000;
DUMP result;








