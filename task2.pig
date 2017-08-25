--Task 2
--Write a Pig Program to calculate the number of cases investigated under FBI Code 32

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
filterSelectCols = FILTER selectCols BY (FBICode == '32') AND (CaseNumber is not null);
--DESCRIBE filterSelectCols;
--OUTPUT -> filterSelectCols: {FBICode: chararray,CaseNumber: chararray}
--DUMP filterSelectCols; 
--OUTPUT -> no records are dumped, because FBICode = 32 is not present in the data set

--Line 5
groupByFBICode = GROUP filterSelectCols ALL;
--DESCRIBE groupByFBICode;
--OUTPUT -> groupByFBICode: {group: chararray,filterSelectCols: {(FBICode: chararray,CaseNumber: chararray)}}
--DUMP groupByFBICode;
--OUTPUT -> no records are dumped, records will be dumped if we use FBICode that is present in dataset

--Line 6
countCases = FOREACH groupByFBICode GENERATE COUNT(filterSelectCols.CaseNumber) as TotalInvestigatedCases;
--DESCRIBE countCases;
--OUTPUT -> countCases: {TotalInvestigatedCases: long}
DUMP countCases;
--OUTPUT -> no records are dumped, records will be dumped if we use FBICode that is present in dataset


-- There are 26 FBICode (27 records including (), not containing FBICode 32) in the dataset, found using below code:
--result = DISTINCT (FOREACH selectCols GENERATE FBICode);
--DUMP result;
/*
(02)
(03)
(05)
(06)
(07)
(09)
(10)
(11)
(12)
(13)
(14)
(15)
(16)
(17)
(18)
(19)
(20)
(22)
(24)
(26)
(01A)
(01B)
(04A)
(04B)
(08A)
(08B)
()
*/

--An Example ----------->>>>>>>>>>>>>>
--for FBICode = 20 
--Above code i.e. from Line 1 to Line 6 will result 
--OUTPUT -> (1267)


/*Explanation of above Line numbers

Line 1
piggybank.jar is registered, so that in-built functions COUNT, SUBSTRING etc. can be used

Line 2
Data is loaded from local file system to pig using "CSVExcelStorage" load function into "loadData" alias

Line 3
Only FBICode and CaseNumber fields are fetched from loadData and explicit type conversion is done and finally data is loaded into "selectCols" alias

Line 4
"filterSelectCols" alias stores only those records where FBICode == 32 and CaseNumber is not empty

Line 5
Grouping is done on the basis of ALL fields and result is stored in "groupByFBICode" alias

Line 6
Count of all records for group field in "groupByFBICode" alias is done and result is stored in "countCases" alias

Line 7 
Dumps the required output stored in "countCases" alias
*/

