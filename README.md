# H1B-Visa-Application-Analysis using Big Data Technologies

The H1B is an employment-based, non-immigrant visa category for temporary foreign workers in the United States. For a foreign national to apply for H1B visa, an US employer must offer a job and petition for H1B visa with the US immigration department. This is the most common visa status applied for and held by international students once they complete college/ higher education (Masters, Ph.D.) and work in a full-time position.

We will be performing analysis on the H1B visa applicants between the years 2011-2016. The dataset has nearly 3 million records.  After analysing the data, we can derive the following facts.

1 a) Is the number of petitions with Data Engineer job title increasing over time?
   b) Find top 5 job titles who are having highest average growth in applications.
       [Case Status-ALL]

2 a) Which part of the US has the most Data Engineer jobs for each year?
   b) find top 5 locations in the US who have got certified visa for each year.[Case Status -Certified]

3)Which industry(SOC_NAME) has the most number of Data Scientist positions?[Case Status -Certified]

4)Which top 5 employers file the most petitions each year? [Case Status – ALL]

 
5) Find the most popular top 10 job positions for H1B visa applications for each year?
a) for all the applications
b) for only certified applications.

6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.

7) Create a bar graph to depict the number of applications for each year [All]

8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]

9) Which are the employers along with the number of petitions who have the success rate more than 70% in petitions. (total petitions filed 1000 OR more than 1000) ? 
          SUCCESS RATE % = (Certified + Certified Withdrawn)/Total x 100
10) Which are the job positions along with the number of petitions which have the success rate more than 70% in petitions (total petitions filed 1000 OR more than 1000)?
 
            SUCCESS RATE % = (Certified + Certified Withdrawn)/Total x 100

11) Export result for question no 10 to MySql database.

Dataset Description


Sr No.	Column Name	Description
1	CASE_STATUS	Status associated with the last significant event or decision. Valid values include “Certified,” “Certified-Withdrawn,” Denied,” and “Withdrawn”.

Certified: Employer filed the LCA(Labour Condition Application), which was approved by DOL(Department of Labor)

Certified Withdrawn: LCA was approved but later withdrawn by employer
Withdrawn: LCA was withdrawn by employer before approval

Denied: LCA was denied by DOL

2	EMPLOYER_NAME	Name of employer submitting Labour Condition Application
3	SOC_NAME	The Occupational name associated with the SOC_CODE. SOC_CODE is the occupational code associated with the job being requested for temporary labour condition, as classified by the Standard Occupational Classification (SOC) System.

4	JOB_TITLE	Title of the job
5	FULL_TIME_POSITION	Y = Full Time Position; N = Part Time Position
6	PREVAILING_WAGE	Prevailing Wage for the job being requested for temporary labour condition. The wage is listed at annual scale in USD. The prevailing wage for a job position is defined as the average wage paid to similarly employed workers in the requested occupation in the area of intended employment.
7	YEAR	Year in which the H1B visa petition was filed
8	WORKSITE	City and State information of the foreign worker’s intended area of employment

9	LONGITUDE	Longitude of the Worksite.
10	LATITUDE	Latitude of the Worksite.

 
Software Requirement

•	Ubuntu16.04 LTS
•	Hadoop 2.6.0
•	Hive 1.2.1
•	Apache Pig version 0.13.0
•	mysql  Ver 14.14 Distrib 5.7.20, for Linux (x86_64)
•	Sqoop 1.4.6
•	java-8-openjdk-amd64
•	Eclipse  3.8.1

