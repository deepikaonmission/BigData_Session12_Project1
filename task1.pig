--Task 1
--Write a Pig Program to calculate the number of cases investigated under each FBI Code

--Line 1
REGISTER '/usr/local/pig/lib/piggybank.jar';

--Line 2
loadData = LOAD '/home/acadgild/Documents/Project1/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX');
--DESCRIBE loadData;
--OUTPUT -> Schema for loadData unknown
--DUMP loadData;
--OUTPUT -> DUMPS 291267 records

--Line 3
selectCols = FOREACH loadData GENERATE (chararray)$14 as FBICode,(chararray)$1 as CaseNumber;
--DESCRIBE selectCols;
--OUTPUT -> selectCols: {FBICode: chararray,CaseNumber: chararray}
--DUMP selectCols;
--OUTPUT -> DUMPS 291267 records

--Line 4
filterSelectCols = FILTER selectCols BY (FBICode is not null) AND (CaseNumber is not null);
--DESCRIBE filterSelectCols;
--OUTPUT -> selectCols: {FBICode: chararray,CaseNumber: chararray}
--DUMP filterSelectCols;
--OUTPUT -> DUMPS 291267 records

--Line 5
groupByFBICode = GROUP filterSelectCols by FBICode;
--DESCRIBE groupByFBICode;
--OUTPUT -> groupByFBICode: {group: chararray,filterSelectCols: {(FBICode: chararray,CaseNumber: chararray)}}
--DUMP groupByFBICode;
--OUTPUT -> DUMPS 26 records

--Line 6
countCases = FOREACH groupByFBICode GENERATE group as FBICode, COUNT(filterSelectCols.CaseNumber) as TotalInvestigatedCases;
--DESCRIBE countCases;
--OUTPUT -> countCases: {FBICode: chararray,TotalInvestigatedCases: long}

--Line 7
DUMP countCases;
--OUTPUT -> DUMPS 26 records

/*Explanation of above Line numbers

Line 1
piggybank.jar is registered, so that in-built functions COUNT, SUBSTRING etc. can be used

Line 2
Data is loaded from local file system to pig using "CSVExcelStorage" load function into "loadData" alias

Line 3
Only FBICode and CaseNumber fields are fetched from loadData and explicit type conversion is done and finally data is loaded into "selectCols" alias

Line 4
"filterSelectCols" alias stores only those records where FBICode is not empty and CaseNumber is not empty

Line 5
Grouping is done on the basis of FBICode and result is stored in "groupByFBICode" alias

Line 6
Count of CaseNumbers is done for each FBICode and result is stored in "countCases" alias

Line 7 
Dumps the required output stored in "countCases" alias
*/
