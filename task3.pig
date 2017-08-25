--Task 3
--Write a Pig Program to calculate the number of arrests in theft district wise

--Line 1
REGISTER '/usr/local/pig/lib/piggybank.jar';

--Line 2
loadData = LOAD '/home/acadgild/Documents/Project1/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX');
--DESCRIBE loadData;
--OUTPUT -> Schema for loadData unknown
--DUMP loadData
--OUTPUT -> DUMPS 291267 records

--Line 3
selectCols = FOREACH loadData GENERATE (int)$11 as District,(chararray)$5 as PrimaryType,(chararray)$8 as Arrest;
--DESCRIBE selectCols;
--OUTPUT -> selectCols: {District: int,PrimaryType: chararray,Arrest: chararray}
--DUMP selectCols;
--OUTPUT -> DUMPS 291267 records

--Line 4
filterSelectCols = FILTER selectCols BY (District is not null) AND (PrimaryType == 'THEFT') AND (Arrest == 'true');
--DESCRIBE filterSelectCols;
--OUTPUT -> filterSelectCols: {District: int,PrimaryType: chararray,Arrest: chararray}
--DUMP filterSelectCols; 
--OUTPUT -> DUMPS 7634 records

--Line 5
groupByFBICode = GROUP filterSelectCols BY District;
--DESCRIBE groupByFBICode;
--OUTPUT -> groupByFBICode: {group: int,filterSelectCols: {(District: int,PrimaryType: chararray,Arrest: chararray)}}
--DUMP groupByFBICode;
--OUTPUT -> DUMPS 22 records

--Line 6
countCases = FOREACH groupByFBICode GENERATE group, COUNT(filterSelectCols.Arrest) as TotalTheftArrests;
--DESCRIBE countCases;
--OUTPUT -> countCases: {group: int,TotalTheftArrests: long}

--Line 7
DUMP countCases;
--OUTPUT -> DUMPS 22 records


/*Explanation of above Line numbers

Line 1
piggybank.jar is registered, so that in-built functions COUNT, SUBSTRING etc. can be used

Line 2
Data is loaded from local file system to pig using "CSVExcelStorage" load function into "loadData" alias

Line 3
Only District, PrimaryType and Arrest fields are fetched from loadData and explicit type conversion is done and finally data is loaded into "selectCols" alias

Line 4
"filterSelectCols" alias stores only those records where District is not empty, PrimaryType is Theft and Arrest value is True

Line 5
Grouping is done on the basis of District field and result is stored in "groupByFBICode" alias

Line 6
Count of thefts with arrests for each District is done and result is stored in "countCases" alias

Line 7 
Dumps the required output stored in "countCases" alias
*/

