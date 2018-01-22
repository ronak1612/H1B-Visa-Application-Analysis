#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1 )${MENU} Q1 a) Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2 )${MENU} Q1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3 )${MENU} Q2 a)Which part of the US has the most Data Engineer jobs for each year?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4 )${MENU} Q2 b)find top 5 locations in the US who have got certified visa for each year.[certified]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5 )${MENU} Q3 )Which industry(SOC_NAME) has the most number of Data Scientist positions?[certified]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6 )${MENU} Q4 )Which top 5 employers file the most petitions each year? - Case Status - ALL${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7 )${MENU} Q5 a)Find the most popular top 10 job positions for H1B visa applications for each year for all the
                                          applications .${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8 )${MENU} Q5 b)Find the most popular top 10 job positions for H1B visa applications for each year for only
					   certified applications.${NORMAL}"
   
    echo -e "${MENU}**${NUMBER} 9)${MENU}  Q6 )Find the percentage and the count of each case status on total applications for each year.
					       Create a line graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} Q7 )Create a bar graph to depict the number of applications for each year [All]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Q8 )Find the average Prevailing Wage for each Job for each Year (take part time and full time
					   separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} Q9 )Which are the employers along with the number of petitions who have the success rate more than
					   70% in petitions. (total petitions filed 1000 OR more than 1000) ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} Q10 )Which are the  job positions along with the number of petitions which have the success rate more
  					   than 70%  in petitions (total petitions filed 1000 OR more than 1000)?[All]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14)${MENU} Q11 )Export result for question no 10 to MySql database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}


clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked " Q1 a) Is the number of petitions with Data Engineer job title increasing over time?";
          echo -e "Enter folder name to store output file:"
        read folder_name
          hadoop jar Q1a.jar /user/hive/warehouse/h1b_final /ProjectOutputs/$folder_name
	show_menu;
        ;;

        2) clear;
        option_picked " Q1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL] ";
          pig -x local Q1b.pig
        show_menu;
        ;;
            

        3) clear;
        option_picked " Q2 a)Which part of the US has the most Data Engineer jobs for each year? ";
          echo -e "Enter folder name to store output file:"
        read folder_name
          hadoop jar Q2a.jar /user/hive/warehouse/h1b_final /ProjectOutputs/$folder_name
        show_menu;
        ;;

        4) clear;
        option_picked "Q2 b)find top 5 locations in the US who have got certified visa for each year.[certified]";

        echo -e "Enter folder name to store output file:"
        read folder_name
          hadoop jar Q2b.jar /user/hive/warehouse/h1b_final /ProjectOutputs/$folder_name
       			show_menu;
				    ;;

	5) clear;
		option_picked "Q3 )Which industry(SOC_NAME) has the most number of Data Scientist positions?[certified]";
		 echo -e "Enter folder name to store output file:"
		read folder_name
		  hadoop jar Q3.jar /user/hive/warehouse/h1b_final /ProjectOutputs/$folder_name  
		  show_menu;
		;;

  	6) clear;
        option_picked " Q4 )Which top 5 employers file the most petitions each year? - Case Status - ALL";
       
        echo -e "${MENU}From which year you want to search Top 5 Employers who filed most petitions? ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 7)${MENU} All Year ${NORMAL}"
        echo "Please Enter the Year"
	    read Year
	    echo "Year is ${Year}"
	    
				    case $Year in
					1)	echo "Top 5 Employers who filed most petions in Year 2011."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2011"
					    ;;		
					    
					2) 	echo "Top 5 Employers who filed most petions in Year 2012."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2012"
					    ;;		
					    
					3) 	echo "Top 5 Employers who filed most petions in Year 2013."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2013"
					    ;;		
					    
					4) 	echo "Top 5 Employers who filed most petions in Year 2014."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2014"
					    ;;		
					    
					5) 	echo "Top 5 Employers who filed most petions in Year 2015."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2015"
					    ;;		
				       
					6) 	echo "Top 5 Employers who filed most petions in Year 2016."
						                        	
					    hive -e "select * from h1b_analysis.top5_employer_2016"
					    ;;	
					7) 	echo "Top 5 Employers who filed most petions in AllYear."
						                        	
					    hive -f "top5employer.sql"
					    ;;		               
			   
					*) echo "Please Select one among the option[1-7]";;
					esac
					show_menu;
					    ;;



	7) clear;
        option_picked "Q5 a)Find the most popular top 10 job positions for H1B visa applications for each year for ALL the applications .";
       
        echo -e "${MENU}From which year you want to display Top 10 job application. ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 7)${MENU} All Year ${NORMAL}"
        echo "Please Enter the Year"
	    read Year
	    echo "Year is ${Year}" 
					    case $Year in
						1)	echo "Top 10 job positions in Year 2011."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2011"
						    ;;		
						    
						2) 	echo "Top 10 job positions in Year 2012."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2012"
						    ;;		
						    
						3) 	echo "Top 10 job positions in Year 2013."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2013"
						    ;;		
						    
						4) 	echo "Top 10 job positions in Year 2014."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2014"
						    ;;		
						    
						5) 	echo "Top 10 job positions in Year 2015."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2015"
						    ;;		
					       
						6) 	echo "Top 10 job positions in Year 2016."
								                	
						    hive -e "select * from h1b_analysis.top10_job_position_2016"
						    ;;		
						7) 	echo "Top 10 job positions AllYear."
								                	
						    hive -f "top10_job_position.sql"
				
						    ;;		
						    	
					        *) echo "Please Select one among the option[1-7]";;
							esac
							show_menu;
							    ;;

	
       
	     
        8) clear;

        option_picked "Q5 b)Find the most popular top 10 job positions for H1B visa applications for each year for only certified
			     applications.";
       
        echo -e "${MENU}From which year you want to display Top 10 job application.${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 7)${MENU} All Year ${NORMAL}"
        echo "Please Enter the Year"
	    read Year
       
	  
			    case $Year in
				1)	echo "Top 10 job positions in Year 2011."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2011_certified"
				    ;;		
				    
				2) 	echo "Top 10 job positions in Year 2012."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2012_certified"
				    ;;		
				    
				3) 	echo "Top 10 job positions in Year 2013."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2013_certified"
				    ;;		
				    
				4) 	echo "Top 10 job positions in Year 2014."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2014_certified"
				    ;;		
				    
				5) 	echo "Top 10 job positions in Year 2015."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2015_certified"
				    ;;		
			       
				6) 	echo "Top 10 job positions in Year 2016."
				                                	
				    hive -e "select * from h1b_analysis.top10_job_position_2016_certified"
				    ;;		
				7) 	echo "Top 10 job positions AllYear."
				                                	
				    hive -f "5b"
				    ;;		
				    	
				 *) echo "Please Select one among the option[1-7]";;
					esac
					show_menu;
					    ;;



        9) clear;
        option_picked "Q6 )Percentage and the count of each case status on total applications for each year. Create a
					 line graph depicting the pattern of All the cases over the period of time.";
        
	    pig -x local "Q6.pig"
            xdg-open LineGraph.png
        show_menu;
        ;;
      
       10)clear;
        option_picked "Q7 )Create a bar graph to depict the number of applications for each year [All].";
          hive -e "Select year, count(*) from h1b_final group by year"
 	  xdg-open BarGraph.png
        show_menu;
        ;;
      

	11) clear;
        option_picked "Q8 )Find the average Prevailing Wage for each Job for each Year (take part time and full time separate).Arrange the output in descending order - [Certified and Certified Withdrawn.]$.";

        echo -e "${MENU}From which year you want to display prevailing wage.${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Provided by User ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} All Year FullTime ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 3)${MENU} All Year Part Time${NORMAL}"
	   echo "Please Enter the Option"
	    read Option
       
	  
			    case $Option in
				1)	echo "Enter Year"
                                        read yr
                                        echo "Enter Position(Full Time-Y, Part-Time-N)"
                                        read ps
				                                	
				        pig -x local -param "year=yr" -param "position=ps" Q8Parameter.pig
				    ;;		
				    
				2) 	echo "All Year Average FullTime Prevailing Wage"
				                                	
				     pig -x local "Q8FullTime.pig"

				    ;;		
				    
				3) 	echo "All Year Average PartTime Prevailing Wage"
				                                	
				     pig -x local "Q8PartTime.pig"

				    ;;	
                                *) echo "Please Select one among the option[1-3]";;
					esac
				    
        show_menu;
        ;;

	
	12) clear;
        option_picked " Q9) Which are the employers along with the number of petitions who have the success rate more than 70% 
                                         in petitions. (total petitions filed 1000 OR more than 1000) ?";
        
	    pig -x local "Q9.pig"
        show_menu;
        ;;


	13) clear;
        option_picked "Q10) Which are the  job positions along with the number of petitions which have the success rate more
  					   than 70%  in petitions (total petitions filed 1000 OR more than 1000)?[All] ";
        
	    pig -x local "Q10.pig"
        show_menu;
        ;;

      
     
    esac
fi



done


