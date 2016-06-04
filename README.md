# Geonames Importer #

This importer is designed to import the data from Geonames.org in to your choice of MySql or ElasticSearch.
It gives the option of either Zip (Zip Code) or Country.

## Requirements ##
For the unzipping process, it requires you have "zip" installed on the server. You can install it via the command below.
`PECL zip install`

If using the mysql importer, you will need to have the following two databases created.

### Geonames (countries)
CREATE TABLE geonames
(
    id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    geonameid INT(10) UNSIGNED NOT NULL,
    name VARCHAR(200) NOT NULL,
    asciiname VARCHAR(200),
    alternatenames VARCHAR(10000),
    latitude FLOAT(10,6),
    longitude FLOAT(10,6),
    feature_class CHAR(1),
    feature_code VARCHAR(10),
    country_code VARCHAR(2),
    cc2 VARCHAR(200),
    admin1_code VARCHAR(20),
    admin2_code VARCHAR(80),
    admin3_code VARCHAR(20),
    admin4_code VARCHAR(20),
    population BIGINT(20),
    elevation INT(11),
    dem VARCHAR(40),
    timezone VARCHAR(40),
    modification_date DATE
);
CREATE UNIQUE INDEX geonameid ON geonames (geonameid);

### Geonames (zip codes)
CREATE TABLE geonames_zips
(
    id INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    country_code VARCHAR(2) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    place_name VARCHAR(180) NOT NULL,
    admin_name1 VARCHAR(100) NOT NULL,
    admin_code1 INT(20) NOT NULL,
    admin_name2 INT(100) NOT NULL,
    admin_code2 INT(20) NOT NULL,
    admin_name3 INT(100) NOT NULL,
    admin_code3 INT(20) NOT NULL,
    latitude FLOAT(10,6) NOT NULL,
    longitude FLOAT(10,6) NOT NULL,
    accuracy INT(1) NOT NULL
);
CREATE UNIQUE INDEX postal_code ON geonames_zips (postal_code);

## How to run app ##
To run the program, you will run the following command, after changing your path in to the root directory of the program.
- `php importer.php -t(type, "zips" or "countries", required), -z(Name of file, default: US, not required), -d(data storage, "elasticsearch" or "mysql", required), -c(chunk insert count, not required, default: 500), --debug(display debug info, not required)


## TODO ##
- Fix "force reload"
- Fix "catch up" (progress is recorded already, this option will allow the program to catchup to where it was)

### README file from countries
 
Readme for GeoNames Gazetteer extract files

============================================================================================================

This work is licensed under a Creative Commons Attribution 3.0 License,
see http://creativecommons.org/licenses/by/3.0/
The Data is provided "as is" without warranty or any representation of accuracy, timeliness or completeness.

The data format is tab-delimited text in utf8 encoding.


Files :
-------
XX.zip                   : features for country with iso code XX, see 'geoname' table for columns
allCountries.zip         : all countries combined in one file, see 'geoname' table for columns
cities1000.zip           : all cities with a population > 1000 or seats of adm div (ca 80.000), see 'geoname' table for columns
cities5000.zip           : all cities with a population > 5000 or PPLA (ca 40.000), see 'geoname' table for columns
cities15000.zip          : all cities with a population > 15000 or capitals (ca 20.000), see 'geoname' table for columns
alternateNames.zip       : two files, alternate names with language codes and geonameId, file with iso language codes
admin1CodesASCII.txt     : ascii names of admin divisions. (beta > http://forum.geonames.org/gforum/posts/list/208.page#1143)
admin2Codes.txt          : names for administrative subdivision 'admin2 code' (UTF8), Format : concatenated codes <tab>name <tab> asciiname <tab> geonameId
iso-languagecodes.txt    : iso 639 language codes, as used for alternate names in file alternateNames.zip
featureCodes.txt         : name and description for feature classes and feature codes 
timeZones.txt            : countryCode, timezoneId, gmt offset on 1st of January, dst offset to gmt on 1st of July (of the current year), rawOffset without DST
countryInfo.txt          : country information : iso codes, fips codes, languages, capital ,...
                           see the geonames webservices for additional country information,
                                bounding box                         : http://ws.geonames.org/countryInfo?
                                country names in different languages : http://ws.geonames.org/countryInfoCSV?lang=it
modifications-<date>.txt : all records modified on the previous day, the date is in yyyy-MM-dd format. You can use this file to daily synchronize your own geonames database.
deletes-<date>.txt       : all records deleted on the previous day, format : geonameId <tab> name <tab> comment.

alternateNamesModifications-<date>.txt : all alternate names modified on the previous day,
alternateNamesDeletes-<date>.txt       : all alternate names deleted on the previous day, format : alternateNameId <tab> geonameId <tab> name <tab> comment.
userTags.zip		: user tags , format : geonameId <tab> tag.
hierarchy.zip		: parentId, childId, type. The type 'ADM' stands for the admin hierarchy modeled by the admin1-4 codes. The other entries are entered with the user interface. The relation toponym-adm hierarchy is not included in the file, it can instead be built from the admincodes of the toponym.


The main 'geoname' table has the following fields :
---------------------------------------------------
geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int) 
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format


AdminCodes:
Most adm1 are FIPS codes. ISO codes are used for US, CH, BE and ME. UK and Greece are using an additional level between country and fips code. The code '00' stands for general features where no specific adm1 code is defined.
The corresponding admin feature is found with the same countrycode and adminX codes and the respective feature code ADMx.



The table 'alternate names' :
-----------------------------
alternateNameId   : the id of this alternate name, int
geonameid         : geonameId referring to id in table 'geoname', int
isolanguage       : iso 639 language code 2- or 3-characters; 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link for a website, varchar(7)
alternate name    : alternate name or name variant, varchar(200)
isPreferredName   : '1', if this alternate name is an official/preferred name
isShortName       : '1', if this is a short name like 'California' for 'State of California'
isColloquial      : '1', if this alternate name is a colloquial or slang term
isHistoric        : '1', if this alternate name is historic and was used in the past

Remark : the field 'alternatenames' in the table 'geoname' is a short version of the 'alternatenames' table without links and postal codes but with ascii transliterations. You probably don't need both. 
If you don't need to know the language of a name variant, the field 'alternatenames' will be sufficient. If you need to know the language
of a name variant, then you will need to load the table 'alternatenames' and you can drop the column in the geoname table.

Statistics on the number of features per country and the feature class and code distributions : http://www.geonames.org/statistics/ 

Continent codes :
AF : Africa			geonameId=6255146
AS : Asia			geonameId=6255147
EU : Europe			geonameId=6255148
NA : North America		geonameId=6255149
OC : Oceania			geonameId=6255151
SA : South America		geonameId=6255150
AN : Antarctica			geonameId=6255152

If you find errors or miss important places, please do use the wiki-style edit interface on our website 
http://www.geonames.org to correct inaccuracies and to add new records. 
Thanks in the name of the geonames community for your valuable contribution.

Data Sources:
http://www.geonames.org/data-sources.html

More Information is also available in the geonames faq :

http://forum.geonames.org/gforum/forums/show/6.page

The forum : http://forum.geonames.org

or the google group : http://groups.google.com/group/geonames



### README file from zips 
Readme for GeoNames Postal Code files :

This work is licensed under a Creative Commons Attribution 3.0 License.
This means you can use the dump as long as you give credit to geonames (a link on your website to www.geonames.org is ok)
see http://creativecommons.org/licenses/by/3.0/
UK: Contains Royal Mail data Royal Mail copyright and database right 2015.
The Data is provided "as is" without warranty or any representation of accuracy, timeliness or completeness.

This readme describes the GeoNames Postal Code dataset.
The main GeoNames gazetteer data extract is here: http://download.geonames.org/export/dump/


For many countries lat/lng are determined with an algorithm that searches the place names in the main geonames database 
using administrative divisions and numerical vicinity of the postal codes as factors in the disambiguation of place names. 
For postal codes and place name for which no corresponding toponym in the main geonames database could be found an average 
lat/lng of 'neighbouring' postal codes is calculated.
Please let us know if you find any errors in the data set. Thanks

For Canada we have only the first letters of the full postal codes (for copyright reasons)

For Ireland we have only the first letters of the full postal codes (for copyright reasons)

For Malta we have only the first letters of the full postal codes (for copyright reasons)

The Argentina data file contains 4-digit postal codes which were replaced with a new system in 1999.

For Brazil only major postal codes are available (only the codes ending with -000 and the major code per municipality).

For India the lat/lng accuracy is not yet comparable to other countries.

The data format is tab-delimited text in utf8 encoding, with the following fields :

country code      : iso country code, 2 characters
postal code       : varchar(20)
place name        : varchar(180)
admin name1       : 1. order subdivision (state) varchar(100)
admin code1       : 1. order subdivision (state) varchar(20)
admin name2       : 2. order subdivision (county/province) varchar(100)
admin code2       : 2. order subdivision (county/province) varchar(20)
admin name3       : 3. order subdivision (community) varchar(100)
admin code3       : 3. order subdivision (community) varchar(20)
latitude          : estimated latitude (wgs84)
longitude         : estimated longitude (wgs84)
accuracy          : accuracy of lat/lng from 1=estimated to 6=centroid
