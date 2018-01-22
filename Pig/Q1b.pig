
ds1 = LOAD '/home/hduser/Part1' USING PigStorage('\t') AS  (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

ds2 = LOAD '/home/hduser/Part2.csv' USING PigStorage('\t') AS (s_no:int, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:int, year:chararray, worksite:chararray, longitute:double, latitute:double);

data = UNION ds1, ds2;

required = FOREACH data GENERATE $7, $4, $1 ; -- year, job_title, case_status,.

Dump required;


--TAKING ONLY 2011 DATA
data2011 = filter required by $0 =='2011';
group2011 = group data2011 by $1;
count2011 = foreach group2011 generate group, COUNT($1) AS Count;
ordercount2011 = order count2011 by Count desc;
--dump ordercount2011;



--TAKING ONLY 2012 DATA
data2012 = filter required by $0 == '2012';

--GROUP BY JOB_TITLE
group2012 = group data2012 by $1;

--GENERATING COUNT FOR EACH JOB TITLE GROUP
count2012 = foreach group2012 generate group,COUNT($1);


--TAKING ONLY 2013 DATA
data2013 = filter required by $0 == '2013';

--GROUP BY JOB_TITLE
group2013 = group data2013 by $1;

--GENERATING COUNT FOR EACH JOB TITLE GROUP
count2013 = foreach group2013 generate group,COUNT($1);


--TAKING ONLY 2014 DATA
data2014 = filter required by $0 == '2014';

--GROUP BY JOB_TITLE
group2014 = group data2014 by $1;

--GENERATING COUNT FOR EACH JOB TITLE GROUP
count2014 = foreach group2014 generate group,COUNT($1);


--TAKING ONLY 2015 DATA
data2015 = filter required by $0 == '2015';

--GROUP BY JOB_TITLE
group2015 = group data2015 by $1;

--GENERATING COUNT FOR EACH JOB TITLE GROUP
count2015 = foreach group2015 generate group,COUNT($1);


--TAKING ONLY 2016 DATA
data2016 = filter required by $0 == '2016';

--GROUP BY JOB_TITLE
group2016 = group data2016 by $1;

--GENERATING COUNT FOR EACH JOB TITLE GROUP
count2016 = foreach group2016 generate group,COUNT($1);

joined= join count2011 by $0,count2012 by $0, count2013 by $0,count2014 by $0,count2015 by $0,count2016 by $0;
describe joined;

yearwiseapplications= foreach joined generate $0,$1,$3,$5,$7,$9,$11;
progressivegrowth= foreach yearwiseapplications  generate $0,ROUND_TO((long)($6-$5)*100/$5,2),ROUND_TO((long)($5-$4)*100/$4,2),ROUND_TO((long)($4-$3)*100/$3,2),ROUND_TO((long)($3-$2)*100/$2,2),ROUND_TO((long)($2-$1)*100/$1,2);

avgprogressivegrowth= foreach progressivegrowth generate $0,($1+$2+$3+$4+$5)/5;

orderedavggrowth= order avgprogressivegrowth by $1 desc;

answer = limit orderedavggrowth  5;

dump answer; 



