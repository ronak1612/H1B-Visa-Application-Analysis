

part1 = LOAD '/home/hduser/Part1' USING PigStorage('\t') as 
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


part2 = LOAD '/home/hduser/Part2.csv' USING PigStorage('\t') as 
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

data = UNION  part1, part2;

DESCRIBE data;
--data: {s_no: int,case_status: chararray,employer_name: chararray,soc_name: chararray,job_title: chararray,full_time_position: chararray,prevailing_wage: int,year: chararray,worksite: chararray,longitute: double,latitute: double}

--Keeping data only where job_title is available i.e removing data with job_title="NA"
finaldata = FILTER data by $4!='NA';


--counting totalrecords/applications per job_title.

a = GROUP finaldata BY job_title;
totalapplications = FOREACH a GENERATE group, COUNT(finaldata.$1) as TOTAL;
--DUMP totalapplications;
--OUTPUT:(JOB_TITLE, COUNT_OF_PETITION_FILED)  
--EACH JOB_TITLE WITH NUMBER OF PETITION_FILED


--Finding total certified application per job_title

b = FILTER finaldata BY $1 == 'CERTIFIED';

b1 = GROUP b by $4;

certifiedapplication = FOREACH b1 GENERATE group, COUNT(b.$1);

--DUMP certifiedapplication;


--Finding total certified-withdrawn application per job_title

c = FILTER finaldata BY $1 == 'CERTIFIED-WITHDRAWN';

c1 = GROUP c by $4;

certifiedwithdrawnapplication = FOREACH c1 GENERATE group, COUNT(c.$1);

DUMP certifiedwithdrawnapplication;




joinedPetition = JOIN certifiedapplication BY $0, certifiedwithdrawnapplication BY $0, totalapplications BY $0;

DESCRIBE joinedPetition;


CalculationTable = FOREACH joinedPetition GENERATE $0, $1, $3, $5;
DESCRIBE CalculationTable;


Success_Rate = FOREACH CalculationTable GENERATE $0, ((($1+$2)*100/$3)) AS SUCCESS_RATE_PERCENTAGE, $3 ;
						

--DESCRIBE Success_Rate;


ordering = ORDER Success_Rate BY SUCCESS_RATE_PERCENTAGE DESC;

--DUMP ordering;

result = FILTER ordering BY (SUCCESS_RATE_PERCENTAGE>70 and TOTAL>=1000);
--DUMP result;


Store result into 'http://localhost:50070/explorer.html#/ProjectOutputs/Q10';




