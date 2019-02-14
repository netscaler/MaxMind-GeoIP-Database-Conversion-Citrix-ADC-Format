# Citrix-ADC-GSLB-GeoIP-Conversion-Tool
Tool to convert Maxming GeoIP City database to Citrix ADC (NetScaler) format
MaxMind GeoIP database cannot be used directly in Citrix ADC. The MaxMind GeoIP database must be converted into NetScaler format and then loaded for IP location detection in GSLB static proximity method and other features like policies.
The script provided converts the MaxMind GeoIP2 database to NetScaler database format.

Steps to convert GeoIP2 database to NetScaler format
1.	Download the script Convert_GeoIPDB_To_Netscaler_Format.pl to a Citrix ADC directory (for example /var/).

2.	Download the .csv format of the GeoIP2 database from https://dev.maxmind.com/geoip/geoip2/geolite2/ and extract the database files into the same directory where you have placed the script.

3.	Unzip the database folder using the following shell command.
tar -xvzf 

4.	Enter  the file name of the database that is to be converted.
The default file names used in the script are that of the Maxmind GeoLite2 City based database.  If you have downloaded GeoLite2 Country or others databases, you must provide the input file names accordingly as listed below.

	-b <filename> name of IPv4 block file to be converted. 
Default file name: GeoLite2-City-Blocks-IPv4.csv

	-i <filename> name of IPv6 block file to be converted. 
Default file name: GeoLite2-City-Blocks-IPv6.csv

	-l <filename> name of location file to be converted. 
Default file name: GeoLite2-City-Locations.csv


5.	Execute  the following shell command to convert the GeoIP2 database format to NetScaler database format.
perl Convert_GeoIPDB_To_Netscaler_Format.pl -help 

The following files are generated after you run the script.

	-o <filename> ipv4 output file. 
Default output file name: Netscaler_Maxmind_GeoIP_DB_IPv4.csv

	-p <filename> ipv6 output file. 
Default output file name: Netscaler_Maxmind_GeoIP_DB_IPv6.csv

	-logfile <filename>
File containing list of events/messages

	-debug prints all the messages to STDOUT
File containing messages that needs to be printed

See Citrix ADC GSLB docs for more details - https://docs.citrix.com/en-us/citrix-adc/12-1/global-server-load-balancing.html
