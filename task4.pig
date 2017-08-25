--Task 4
--Write a Pig Program to calculate the number of arrests done between October 2014 and October 2015

--Line 1
REGISTER '/usr/local/pig/lib/piggybank.jar';

--Line 2
loadData = LOAD '/home/acadgild/Documents/Project1/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX');
--DESCRIBE loadData;
--OUTPUT -> Schema for loadData unknown
--DUMP loadData;
--OUTPUT -> DUMPS 291267 records

--Line 3
selectCols = FOREACH loadData GENERATE (chararray)$2 as DtTime,(chararray)$8 as Arrest;
--DESCRIBE selectCols;
--OUTPUT -> selectCols: {DtTime: chararray,Arrest: chararray}
--DUMP selectCols;
--OUTPUT -> DUMPS 291267 records

--Line 4
filterSelectCols = FILTER selectCols BY (DtTime is not null) AND (Arrest == 'true');
--DESCRIBE filterSelectCols;
--OUTPUT -> filterSelectCols: {DtTime: chararray,Arrest: chararray}
--DUMP filterSelectCols; 
--OUTPUT -> DUMPS 78330 records

--Line 5
dateSubstring = FOREACH filterSelectCols GENERATE ToDate(SUBSTRING(DtTime,0,19),'MM/dd/yyyy hh:mm:ss') as Dt,Arrest;
--DESCRIBE dateSubstring;
--Output -> dateSubstring: {Dt: datetime,Arrest: chararray}
--DUMP dateSubstring;
--OUTPUT -> DUMPS 78330 records


--Line 6
dateWiseArrest = FOREACH dateSubstring GENERATE GetMonth(Dt) as Month,GetYear(Dt) as Year,Arrest;
--DESCRIBE dateWiseArrest;
--Output -> dateWiseArrest: {Month: int,Year: int,Arrest: chararray}
--DUMP dateWiseArrest;
--OUTPUT -> DUMPS 78330 records

--Line 7
totalArrest = FILTER dateWiseArrest BY (Month>9 AND Year == 2014) OR (Month<11 and Year == 2015);
--DESCRIBE totalArrest;
--OUTPUT -> totalArrest: {Month: int,Year: int,Arrest: chararray}
--DUMP totalArrest;
--OUTPUT -> DUMPS 65028 records

--Line 8
groupTotalArrest = GROUP totalArrest ALL;
--DESCRIBE groupTotalArrest;
--OUTPUT -> groupTotalArrest: {group: chararray,totalArrest: {(Month: int,Year: int,Arrest: chararray)}}
--DUMP groupTotalArrest;
--OUTPUT -> DUMPS 1 record

--Line 9
countTotalArrest = FOREACH groupTotalArrest GENERATE COUNT(totalArrest.Arrest) as TotalArrests;
--DESCRIBE countTotalArrest;
--OUTPUT -> countTotalArrest: {TotalArrests: long}

--Line 10
DUMP countTotalArrest;
--OUTPUT -> DUMPS 1 record

/*Explanation of above Line numbers

Line 1
piggybank.jar is registered, so that in-built functions COUNT, SUBSTRING etc. can be used

Line 2
Data is loaded from local file system to pig using "CSVExcelStorage" load function into "loadData" alias

Line 3
Only Date(here DtTime) and Arrest fields are fetched from loadData and explicit type conversion is done and finally data is loaded into "selectCols" alias

Line 4
"filterSelectCols" alias stores only those records where DtTime is not empty and Arrest value is True

Line 5
First of all substring function fetches only date and time from DtTime field then ToDate function converts the fetched date and time (of chararray type) to datetime data type which is given an alias Dt and finally Dt and Arrest records are stored in "dateSubstring" alias

Line 6
Here, month and year are fetched from Dt field and are given aliases Month and Year finally Month, Year and Arrest records are stored in "dateWiseArrest" alias

Line 7
Records are filtered i.e. data between October 2014 to October 2015 is fetched and stored in "totalArrest" alias

Line 8
Grouping is done all fields and result is stored in "groupTotalArrest" alias

Line 9
Count of total arrests between October 2014 to October 2015 is done and result is stored in "countTotalArrest" alias

Line 10 
Dumps the required output stored in "countTotalArrest" alias
*/



