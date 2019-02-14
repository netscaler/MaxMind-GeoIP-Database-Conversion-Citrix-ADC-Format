#This file is used for merging the Geo IP DB provided at http://dev.maxmind.com/geoip/geoip2/geolite2/  to netscaler format. 
use strict;
no warnings;
use Getopt::Long;
use Socket;
use File::Basename;
use LWP::Simple qw($ua getstore is_success);
use URI;
#use Archive::Zip;
use Time::Piece;
use MIME::Lite;
use Digest::MD5;
use Cwd;
use Net::IP qw(ip_range_to_prefix);

## Variables -------------------------------------------------------------------------------

## location_id as key to store location attrubutes
our (%location,%geoname_id_hash);
my (%options,$debug);
our ($block_file,$block_file_v6,$location_file,$output_file,$output_file_v6,$log_file);

##-----------------------------------------------------------------------------------------------
## Main :: Read the input block and location files and generate the DB in netscaler format 
##-----------------------------------------------------------------------------------------------
main();

sub main
{

	GetOptions(\%options,"o=s","p=s","b=s","i=s","l=s","logfile=s","help","debug");
	$output_file = exists $options{o}? $options{o}:"Netscaler_Maxmind_GeoIP_DB_IPv4.csv";
	$output_file_v6 = exists $options{p}? $options{p}:"Netscaler_Maxmind_GeoIP_DB_IPv6.csv";
	$log_file = exists $options{logfile}? $options{logfile}:"logfile.txt";	
	$block_file = exists $options{b}? $options{b}:"GeoLite2-City-Blocks-IPv4.csv";
	$block_file_v6 = exists $options{i}? $options{i}:"GeoLite2-City-Blocks-IPv6.csv";
	$location_file = exists $options{l}? $options{l}:"GeoLite2-City-Locations-en.csv";	
	$debug = exists $options{debug}?1:0;
	

	if(exists $options{help}){
		HELP(); 
	}
	
=head
	if(-e $output_file){
                print "$output_file already exists.Overwrite it(y/n)?";
                while(1){
                my $response = $ARGV[0];#<STDIN>; 
		#warn "$output_file already exists.Overwrite it(y/n) $ARGV[0]";
                chomp($response);
                if($response =~ /^\s*N\s*$/i){ #warn "no";
                        exit;
                }
                elsif($response =~ /^\s*Y\s*$/i){
                        `rm -f $output_file`; 
                        last;
                }
                else{
                        #warn "Invalid response.Please enter Y or N. ";
			printlog("No response received, overwriting output file.\n");
			last;
                }
                }
        }
		
	if(-e $output_file_v6){
                print "$output_file_v6 already exists.Overwrite it(y/n)?";
                while(1){
                my $response = $ARGV[0];#<STDIN>; 
		#warn "$output_file already exists.Overwrite it(y/n) $ARGV[0]";
                chomp($response);
                if($response =~ /^\s*N\s*$/i){ #warn "no";
                        exit;
                }
                elsif($response =~ /^\s*Y\s*$/i){
                        `rm -f $output_file_v6`; 
                        last;
                }
                else{
                        #warn "Invalid response.Please enter Y or N. ";
			printlog("No response received, overwriting output file.\n");
			last;
                }
                }
        }
=cut
    
    open (LOG,">$log_file") or die "Cannot open logfile:$!\n";
	#$date = localtime->strftime('%m/%d/%Y');
	my $datestring = localtime();
        printlog("Executing the script to convert database to netscaler format on $datestring.\n");
		
	#calculate_md5($filename);
	my $in_location_file = $location_file;
    my $out_location_parsed_file = $location_file."_Parsed";
	
	if(-e $out_location_parsed_file){
		printlog("$out_location_parsed_file already exists. Removing it.\n");
		`rm -f $out_location_parsed_file`;
	}

	## location and block files are looked up in the order: from downloaded zip file, if download option
	## not provided, then from provided zip file, if zip file not provided then from specified block/location file in specified directory.
	if(!(-e $block_file) || !(-e $location_file)){
		printlog("Block file or location file not found.Please check $block_file and $location_file exists.\n");
                exit_conversion();
        }

        open (INF, "<$in_location_file") or die "Cannot open location input file $in_location_file:$!";
        open (POUT, ">$out_location_parsed_file") or die "Cannot open location output parsed file $out_location_parsed_file:$!";
        
        printlog("Preparing the Locations File to remove the unwanted Commas in Locations\n");
        foreach my $line(<INF>){
                ## skip empty line
                if($line =~ /^\s*$/){
                        next;
                }

		my @param = split(/\"/,$line);

		if(scalar(@param)>1)
		{
		for(my $iter =1;$iter<=scalar(@param);$iter+=2)
		{
		   $param[$iter]=~s/\,/-/g ;

		}
		}

		my $str=$param[0];
		for(my $iter =1;($iter<scalar(@param)-1);$iter++)
		{
		   $str="$str\"$param[$iter]";
		}
		print POUT "$str";
	}
	
	printlog("Removing Unprintable Characters from the CSV Files.\n");
	my $cmd = "perl -i.bk -pe 's/[^[:ascii:]]//g;' $out_location_parsed_file";
	my $out = `$cmd`;

	my $cmd = "perl -i.bk -pe 's/[^[:ascii:]]//g;' $block_file";
	my $out = `$cmd`;

	close(INF);
	close(POUT);
	
	open (OUT,">$output_file") or die "Cannot open output file:$!\n";
	geoip_city_to_netscaler($block_file,$out_location_parsed_file);
	close(OUT);
	
	my $out_location_parsed_file_v6 = $location_file."IPv6_Parsed";
	
	#Convert IPv6 file
	if(!(-e $block_file_v6) || !(-e $location_file)){
		printlog("Block file or location file not found.Please check $block_file_v6 and $location_file exists.\n");
                exit_conversion();
        }

        open (INF, "<$in_location_file") or die "Cannot open location input file $in_location_file:$!";
        open (POUT, ">$out_location_parsed_file_v6") or die "Cannot open location output parsed file $out_location_parsed_file_v6:$!";
        
        printlog("Preparing the Locations File to remove the unwanted Commas in Locations\n");
        foreach my $line(<INF>){
                ## skip empty line
                if($line =~ /^\s*$/){
                        next;
                }

		my @param = split(/\"/,$line);

		if(scalar(@param)>1)
		{
		for(my $iter =1;$iter<=scalar(@param);$iter+=2)
		{
		   $param[$iter]=~s/\,/-/g ;

		}
		}

		my $str=$param[0];
		for(my $iter =1;($iter<scalar(@param)-1);$iter++)
		{
		   $str="$str\"$param[$iter]";
		}
		print POUT "$str";
	}
	
	printlog("Removing Unprintable Characters from the CSV Files.\n");
	my $cmd = "perl -i.bk -pe 's/[^[:ascii:]]//g;' $out_location_parsed_file_v6";
	my $out = `$cmd`;

	my $cmd = "perl -i.bk -pe 's/[^[:ascii:]]//g;' $block_file_v6";
	my $out = `$cmd`;

	close(INF);
	close(POUT);
	
	open (OUT,">$output_file_v6") or die "Cannot open output file:$!\n";
	geoip_city_to_netscaler_v6($block_file_v6,$out_location_parsed_file_v6);
	close(OUT);
	exit_conversion(); 
}



sub geoip_city_to_netscaler
{
	my ($block_file,$location_file) = @_;
	
	my ($geoname_id_index,$locale_index,$continent_code_index,$continent_name_index,$country_iso_code_index,$country_name_index,$subdivision_1_name_index,
		$subdivision_2_name_index,$city_name_index) = (0,1,2,3,4,5,7,9,10);
	my ($network_start_ip_index,$network_mask_length_index,$_geoname_id_index,$reg_country_geoname_id_index,$latitude_index,$longitude_index) = (0,0,2,3,6,7);
	my ($i,$line,$line_no1,$line_no2,$start_ip,$end_ip,$format_flag,$start_invalid_addr,$last_invalid_addr);

	printlog("Block file :$block_file Location file name: $location_file has been received for conversion.\n");	
	open (INFILE1, "<$block_file") or die "\tCannot open block input file:$!";
 	open (INFILE2, "<$location_file") or die "\tCannot open location input file:$!";

	$line_no1 = $line_no2 = 0;

	print OUT "NSGEO1.0\nQualifier1=Continent\nQualifier2=Country_Code\nQualifier3=Subdivision_1_Name\nQualifier4=Subdivision_2_Name\nQualifier5=City\nStart\n";
	print "Converting the file to netscaler format....\n";

	foreach $line(<INFILE2>){
		my (@index,@parameter);
		#my ($geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_iso_code,$subdivision_name,$city_name)
		#	= (-1,-1,-1,-1,-1,-1,-1,-1);
		my ($geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_1_name,$subdivision_2_name,$city_name)
			= (-1,-1,-1,-1,-1,-1,-1,-1);
		$line_no1++;
		## skip empty line
		if($line =~ /^\s*$/){
			next;
		}
		elsif( $line =~ /^\s*(geoname_id|country_iso_code|subdivision_1_name|subdivision_2_name|city_name)/i && $format_flag != 1){
			@index = split(/\,/,$line);
			for($i = 0;$i<scalar(@index);$i++){
       				#print "$index[$i]\n";
		       		if($index[$i] =~ /^\s*geoname_id/i){ $geoname_id_index = $i;}
			        elsif($index[$i] =~ /continent_code/i){ $continent_code_index = $i;}
			        elsif($index[$i] =~ /continent_name/i){ $continent_name_index = $i;}
			        elsif($index[$i] =~ /country_iso_code/i){ $country_iso_code_index = $i;}
		        	elsif($index[$i] =~ /country_name/i) {$country_name_index = $i;}
			        elsif($index[$i] =~ /subdivision_1_name/i) {$subdivision_1_name_index = $i;}
			        elsif($index[$i] =~ /subdivision_2_name/i) {$subdivision_2_name_index = $i;}
			        elsif($index[$i] =~ /city_name/i){ $city_name_index = $i;}

			}
			printlog("Fields are present at indexes geoname_id: $geoname_id_index, country_iso_code: $country_iso_code_index, country_name: $country_name_index, ".
				"subdivision_1_name: $subdivision_1_name_index, subdivision_2_name: $subdivision_2_name_index, city_name: $city_name_index\n");
			$format_flag = 1; 
			#print "$geoname_id_index,$continent_code_index\n";
			
		}
		else{ 
			if($format_flag != 1){
				printlog("Format not specified in file $location_file. Using default format indexes.\n");
			}
			@parameter = split(/\,/,$line);
			$geoname_id = $parameter[$geoname_id_index];
			$continent_code = $parameter[$continent_code_index];
			$continent_name = $parameter[$continent_name_index];
			$country_iso_code = $parameter[$country_iso_code_index];
			$country_name = $parameter[$country_name_index];
			$subdivision_1_name = $parameter[$subdivision_1_name_index];
			$subdivision_2_name = $parameter[$subdivision_2_name_index];
			$city_name = $parameter[$city_name_index];

			#print "$geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_iso_code,$subdivision_name,$city_name\n";
			$geoname_id_hash{$geoname_id} = 1;
			$location{$geoname_id}{'continent_code'} = $continent_code;
			$location{$geoname_id}{'country_iso_code'} = $country_iso_code;
			$location{$geoname_id}{'subdivision_1_name'} = $subdivision_1_name;
			$location{$geoname_id}{'subdivision_2_name'} = $subdivision_2_name;
			$location{$geoname_id}{'city_name'} = $city_name;
			
		}
	
	}

	$format_flag = 0;
	
	foreach $line(<INFILE1>){
		my (@index,@parameter);
		my ($network_start_ip,$network_mask_length,$_geoname_id,$registered_country_geoname_id,$latitude,$longitude) = (-1,-1,-1,-1,-1,-1);
		$line_no2++;
		## skip empty line
                if($line =~ /^\s*$/){
                        next;
                }
		elsif($line =~ /[a-zA-Z\_]\,/g && $format_flag != 1){
                        
			@index = split(/\,/,$line);
                        for($i = 0;$i<scalar(@index);$i++){
				## changed format of database
				#if($index[$i] =~ /network_start_ip/i){ $network_start_ip_index = $i;}
				#elsif($index[$i] =~ /network_mask_length/i){ $network_mask_length_index = $i;}
				if($index[$i] =~ /network\/\d+/i){ $network_start_ip_index = $i;}
                                elsif($index[$i] =~ /^\s*geoname_id/i){ $_geoname_id_index = $i;}
				elsif($index[$i] =~ /registered_country_geoname_id/i) {$reg_country_geoname_id_index = $i;}
                                elsif($index[$i] =~ /latitude/i){ $latitude_index = $i;}
                                elsif($index[$i] =~ /longitude/i){ $longitude_index = $i;}

			}
			printlog("Fields are present at indexes: network_start_ip: $network_start_ip_index, network_mask_length: $network_mask_length_index, ".
				"geoname_id: $_geoname_id_index, registered_country_geoname_id: $reg_country_geoname_id_index,"." latitude: $latitude_index, longitude: $longitude_index\n");
			$format_flag = 1;
                             
                }
		## for IPv4 range
                #elsif($line =~ /^\s*[\:fF0]+:([\d+\.]+)\,/){ 
		elsif($line =~ /\s*[\d+\.]{3}\d+\/\d+\,/){
			chomp $line;
			if($format_flag != 1){
                                printlog("Format not specified in file $block_file. Using default format indexes.\n");
                        }

			@parameter = split(/\,/, $line);
			($network_start_ip,$network_mask_length) = split('/',$parameter[$network_start_ip_index]);
			#$network_mask_length = $parameter[$network_mask_length_index];
			$_geoname_id = $parameter[$_geoname_id_index];
			$registered_country_geoname_id = $parameter[$reg_country_geoname_id_index];
			$latitude = $parameter[$latitude_index];
			$longitude = $parameter[$longitude_index];
			#print "Parameters are :$parameter[$network_start_ip_index],$parameter[$network_mask_length_index],$parameter[$_geoname_id_index]\n";
			if($network_start_ip =~ /(\d+\.\d+\.\d+\.\d+)/){
				$network_start_ip = $1;
			}
			#print "start ip is $network_start_ip\n";
			($start_ip,$end_ip) = GetIPRange($network_start_ip,$network_mask_length,$line_no2);
			#print "$network_start_ip,$network_mask_length,$_geoname_id,$latitude,$longitude\n";
			if($start_ip == -1 || $end_ip == -1){ next;}	

			##if geoname_id is not specified, use egistered_country_geoname_id
			if($_geoname_id eq ""){
				$_geoname_id = $registered_country_geoname_id;
			}

			if(!exists $geoname_id_hash{$_geoname_id}){
				print LOG "geoname_id \"$_geoname_id\" at line $line_no2 is not present in location file.\n";
				next;
			}

			my $out_continent_code = $location{$_geoname_id}{'continent_code'};
			my $out_county_code = $location{$_geoname_id}{'country_iso_code'}; 
			#my $out_sub_dv = $location{$_geoname_id}{'subdivision_name'};
			my $out_sub_dv_1 = $location{$_geoname_id}{'subdivision_1_name'};
			my $out_sub_dv_2 = $location{$_geoname_id}{'subdivision_2_name'};
			my $out_city = $location{$_geoname_id}{'city_name'};
	
			my $logmsg = "";	
			##Extracting the 31 characters as more than 31 characters are not allowed in a single qualifier.
			if (length($out_county_code) > 32) {
				$logmsg .= "country_code is greater than 32 char ";
				my $tmp_out_county_code = substr($out_county_code, 0, 31); 
				if ( ($out_county_code =~/^"/) && ($tmp_out_county_code !~/\$"/) ) {
					$tmp_out_county_code=$tmp_out_county_code."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_county_code = $tmp_out_county_code;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_sub_dv_1) > 32) {
				$logmsg .= "subdivision_1_name is greater than 32 char ";
				my $tmp_out_sub_dv = substr($out_sub_dv_1, 0, 31); 
				if ( ($out_sub_dv_1 =~/^"/) && ($tmp_out_sub_dv !~/\$"/)) {
					$tmp_out_sub_dv=$tmp_out_sub_dv."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_sub_dv_1 = $tmp_out_sub_dv;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_sub_dv_2) > 32) {
				$logmsg .= "subdivision_2_name is greater than 32 char ";
				 my $tmp_out_sub_dv = substr($out_sub_dv_2, 0, 31);
				if ( ($out_sub_dv_2 =~/^"/) && ($tmp_out_sub_dv !~/\$"/)) {
					$tmp_out_sub_dv=$tmp_out_sub_dv."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_sub_dv_2 = $tmp_out_sub_dv;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_city) > 32) {
				$logmsg .= "city_name is greater than 32 char ";
				my $tmp_out_city = substr($out_city, 0, 31); 
				if ( ($out_city =~/^"/) && ($tmp_out_city !~/\$"/)) {
					$tmp_out_city=$tmp_out_city."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_city = $tmp_out_city;
				$logmsg .= "at line $line_no2\n";
			}
			print LOG $logmsg;
			 #print OUT "$start_ip,$end_ip,,$out_county_code,$out_sub_dv,$out_city,,,$longitude,$latitude\n";
			print OUT "$start_ip,$end_ip,$out_continent_code,$out_county_code,$out_sub_dv_1,$out_sub_dv_2,$out_city,,$longitude,$latitude\n";
		}
		else{
			if(!defined $start_invalid_addr){
				$start_invalid_addr = $last_invalid_addr = $line_no2;
			}else{
				if(($line_no2 - $last_invalid_addr) == 1){
					$last_invalid_addr = $line_no2;
				}else{
					 print LOG "Invalid IPv4 addresses found at lines $start_invalid_addr - $last_invalid_addr.\n";
					 $start_invalid_addr = $last_invalid_addr = $line_no2;
				}
			}
			
		}

	}

	printlog("Invalid IPv4 address found at line $start_invalid_addr - $last_invalid_addr.\n");
	printlog("Processed $line_no2 lines of file $block_file.\n");
	printlog("Gzipping the file\n");
	my $cmd = "gzip -f $output_file ";
	my $output = `$cmd`;
	
	close(INFILE1);
	close(INFILE2);
	
}


sub geoip_city_to_netscaler_v6
{
	my ($block_file,$location_file) = @_;
	
	my ($geoname_id_index,$locale_index,$continent_code_index,$continent_name_index,$country_iso_code_index,$country_name_index,$subdivision_1_name_index,
		$subdivision_2_name_index,$city_name_index) = (0,1,2,3,4,5,7,9,10);
	my ($network_start_ip_index,$network_mask_length_index,$_geoname_id_index,$reg_country_geoname_id_index,$latitude_index,$longitude_index) = (0,0,2,3,6,7);
	my ($i,$line,$line_no1,$line_no2,$start_ip,$end_ip,$format_flag,$start_invalid_addr,$last_invalid_addr);

	printlog("Block file :$block_file Location file name: $location_file has been received for conversion.\n");	
	open (INFILE1, "<$block_file") or die "\tCannot open block input file:$!";
 	open (INFILE2, "<$location_file") or die "\tCannot open location input file:$!";

	$line_no1 = $line_no2 = 0;

	print OUT "NSGEO1.0\nQualifier1=Continent\nQualifier2=Country_Code\nQualifier3=Subdivision_1_Name\nQualifier4=Subdivision_2_Name\nQualifier5=City\nStart\n";
	print "Converting the file to netscaler format....\n";

	foreach $line(<INFILE2>){
		my (@index,@parameter);
		#my ($geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_iso_code,$subdivision_name,$city_name)
		#	= (-1,-1,-1,-1,-1,-1,-1,-1);
		my ($geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_1_name,$subdivision_2_name,$city_name)
			= (-1,-1,-1,-1,-1,-1,-1,-1);
		$line_no1++;
		## skip empty line
		if($line =~ /^\s*$/){
			next;
		}
		elsif( $line =~ /^\s*(geoname_id|country_iso_code|subdivision_1_name|subdivision_2_name|city_name)/i && $format_flag != 1){
			@index = split(/\,/,$line);
			for($i = 0;$i<scalar(@index);$i++){
       				#print "$index[$i]\n";
		       		if($index[$i] =~ /^\s*geoname_id/i){ $geoname_id_index = $i;}
			        elsif($index[$i] =~ /continent_code/i){ $continent_code_index = $i;}
			        elsif($index[$i] =~ /continent_name/i){ $continent_name_index = $i;}
			        elsif($index[$i] =~ /country_iso_code/i){ $country_iso_code_index = $i;}
		        	elsif($index[$i] =~ /country_name/i) {$country_name_index = $i;}
			        elsif($index[$i] =~ /subdivision_1_name/i) {$subdivision_1_name_index = $i;}
			        elsif($index[$i] =~ /subdivision_2_name/i) {$subdivision_2_name_index = $i;}
			        elsif($index[$i] =~ /city_name/i){ $city_name_index = $i;}

			}
			printlog("Fields are present at indexes geoname_id: $geoname_id_index, country_iso_code: $country_iso_code_index, country_name: $country_name_index, ".
				"subdivision_1_name: $subdivision_1_name_index, subdivision_2_name: $subdivision_2_name_index, city_name: $city_name_index\n");
			$format_flag = 1; 
			#print "$geoname_id_index,$continent_code_index\n";
			
		}
		else{ 
			if($format_flag != 1){
				printlog("Format not specified in file $location_file. Using default format indexes.\n");
			}
			@parameter = split(/\,/,$line);
			$geoname_id = $parameter[$geoname_id_index];
			$continent_code = $parameter[$continent_code_index];
			$continent_name = $parameter[$continent_name_index];
			$country_iso_code = $parameter[$country_iso_code_index];
			$country_name = $parameter[$country_name_index];
			$subdivision_1_name = $parameter[$subdivision_1_name_index];
			$subdivision_2_name = $parameter[$subdivision_2_name_index];
			$city_name = $parameter[$city_name_index];

			#print "$geoname_id,$continent_code,$continent_name,$country_iso_code,$country_name,$subdivision_iso_code,$subdivision_name,$city_name\n";
			$geoname_id_hash{$geoname_id} = 1;
#			$location{$geoname_id}{'continent_code'} = $continent_code;^M
			$location{$geoname_id}{'country_iso_code'} = $country_iso_code;
			$location{$geoname_id}{'subdivision_1_name'} = $subdivision_1_name;
			$location{$geoname_id}{'subdivision_2_name'} = $subdivision_2_name;
			$location{$geoname_id}{'city_name'} = $city_name;
			
		}
	
	}

	$format_flag = 0;
	
	foreach $line(<INFILE1>){
		my (@index,@parameter);
		my ($network_start_ip,$network_mask_length,$_geoname_id,$registered_country_geoname_id,$latitude,$longitude) = (-1,-1,-1,-1,-1,-1);
		$line_no2++;
		## skip empty line
                if($line =~ /^\s*$/){
			print "Help";
                        next;
                }
		elsif($line =~ /[a-zA-Z\_]\,/g && $format_flag != 1){
                        
			@index = split(/\,/,$line);
                        for($i = 0;$i<scalar(@index);$i++){
				## changed format of database
				#if($index[$i] =~ /network_start_ip/i){ $network_start_ip_index = $i;}
				#elsif($index[$i] =~ /network_mask_length/i){ $network_mask_length_index = $i;}
				if($index[$i] =~ /network\/\d+/i){ $network_start_ip_index = $i;}
                                elsif($index[$i] =~ /^\s*geoname_id/i){ $_geoname_id_index = $i;}
				elsif($index[$i] =~ /registered_country_geoname_id/i) {$reg_country_geoname_id_index = $i;}
                                elsif($index[$i] =~ /latitude/i){ $latitude_index = $i;}
                                elsif($index[$i] =~ /longitude/i){ $longitude_index = $i;}

			}
			printlog("Fields are present at indexes: network_start_ip: $network_start_ip_index, network_mask_length: $network_mask_length_index, ".
				"geoname_id: $_geoname_id_index, registered_country_geoname_id: $reg_country_geoname_id_index,"." latitude: $latitude_index, longitude: $longitude_index\n");
			$format_flag = 1;
                             
                }
		## for IPv4 range
                #elsif($line =~ /^\s*[\:fF0]+:([\d+\.]+)\,/){ 
		#elsif($line =~ /\s*[\d+\.]{3}\d+\/\d+\,/){
		elsif($line =~ /(([0-9a-fA-F]{1,4}:){1,7}:|                       
([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|        
([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|       
([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|
([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|
([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|
([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|
[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|     
:((:[0-9a-fA-F]{1,4}){1,7}|:)|                   
fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|   
::(ffff(:0{1,4}){0,1}:){0,1}
((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}
(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|        
([0-9a-fA-F]{1,4}:){1,4}:
((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}
(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])         
)\/\d+\,/){
			chomp $line;
			if($format_flag != 1){
                                printlog("Format not specified in file $block_file. Using default format indexes.\n");
                        }

			@parameter = split(/\,/, $line);
			($network_start_ip,$network_mask_length) = split('/',$parameter[$network_start_ip_index]);
			#$network_mask_length = $parameter[$network_mask_length_index];
			$_geoname_id = $parameter[$_geoname_id_index];
			$registered_country_geoname_id = $parameter[$reg_country_geoname_id_index];
			$latitude = $parameter[$latitude_index];
			$longitude = $parameter[$longitude_index];
			#print "Parameters are :$parameter[$network_start_ip_index],$parameter[$network_mask_length_index],$parameter[$_geoname_id_index]\n";
			
			#Commented by Deepak
			#if($network_start_ip =~ /(\d+\.\d+\.\d+\.\d+)/){
			#	$network_start_ip = $1;
			#}
			#print "start ip is $network_start_ip\n";
			#($start_ip,$end_ip) = GetIPRange($network_start_ip,$network_mask_length,$line_no2);
			($start_ip,$end_ip) = Net::IP::ip_prefix_to_range($network_start_ip,$network_mask_length,6);
			#print "$network_start_ip,$network_mask_length,$_geoname_id,$latitude,$longitude\n";
			if($start_ip == -1 || $end_ip == -1){ next;}	

			##if geoname_id is not specified, use egistered_country_geoname_id
			if($_geoname_id eq ""){
				$_geoname_id = $registered_country_geoname_id;
			}

			if(!exists $geoname_id_hash{$_geoname_id}){
				print LOG "geoname_id \"$_geoname_id\" at line $line_no2 is not present in location file.\n";
				next;
			}

		#	my $out_continent_code = $location{$_geoname_id}{'continent_code'};^M
			my $out_county_code = $location{$_geoname_id}{'country_iso_code'}; 
			#my $out_sub_dv = $location{$_geoname_id}{'subdivision_name'};
			my $out_sub_dv_1 = $location{$_geoname_id}{'subdivision_1_name'};
			my $out_sub_dv_2 = $location{$_geoname_id}{'subdivision_2_name'};
			my $out_city = $location{$_geoname_id}{'city_name'};
	
			my $logmsg = "";	
			##Extracting the 31 characters as more than 31 characters are not allowed in a single qualifier.
			if (length($out_county_code) > 32) {
				$logmsg .= "country_code is greater than 32 char ";
				my $tmp_out_county_code = substr($out_county_code, 0, 31); 
				if ( ($out_county_code =~/^"/) && ($tmp_out_county_code !~/\$"/) ) {
					$tmp_out_county_code=$tmp_out_county_code."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_county_code = $tmp_out_county_code;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_sub_dv_1) > 32) {
				$logmsg .= "subdivision_1_name is greater than 32 char ";
				my $tmp_out_sub_dv = substr($out_sub_dv_1, 0, 31); 
				if ( ($out_sub_dv_1 =~/^"/) && ($tmp_out_sub_dv !~/\$"/)) {
					$tmp_out_sub_dv=$tmp_out_sub_dv."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_sub_dv_1 = $tmp_out_sub_dv;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_sub_dv_2) > 32) {
				$logmsg .= "subdivision_2_name is greater than 32 char ";
				 my $tmp_out_sub_dv = substr($out_sub_dv_2, 0, 31);
				if ( ($out_sub_dv_2 =~/^"/) && ($tmp_out_sub_dv !~/\$"/)) {
					$tmp_out_sub_dv=$tmp_out_sub_dv."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_sub_dv_2 = $tmp_out_sub_dv;
				$logmsg .= "at line $line_no2\n";
			}
			if (length($out_city) > 32) {
				$logmsg .= "city_name is greater than 32 char ";
				my $tmp_out_city = substr($out_city, 0, 31); 
				if ( ($out_city =~/^"/) && ($tmp_out_city !~/\$"/)) {
					$tmp_out_city=$tmp_out_city."\"";
					$logmsg .= "and has \"\" in it ";
				}
				$out_city = $tmp_out_city;
				$logmsg .= "at line $line_no2\n";
			}
			print LOG $logmsg;
			 #print OUT "$start_ip,$end_ip,,$out_county_code,$out_sub_dv,$out_city,,,$longitude,$latitude\n";
			print OUT "$start_ip,$end_ip,$out_county_code,$out_sub_dv_1,$out_sub_dv_2,$out_city,,$longitude,$latitude\n";
		}
		else{
			if(!defined $start_invalid_addr){
				$start_invalid_addr = $last_invalid_addr = $line_no2;
			}else{
				if(($line_no2 - $last_invalid_addr) == 1){
					$last_invalid_addr = $line_no2;
				}else{
					 print LOG "Invalid IPv4 addresses found at lines $start_invalid_addr - $last_invalid_addr.\n";
					 $start_invalid_addr = $last_invalid_addr = $line_no2;
				}
			}
			
		}

	}

	printlog("Invalid IPv4 address found at line $start_invalid_addr - $last_invalid_addr.\n");
	printlog("Processed $line_no2 lines of file $block_file.\n");
	printlog("Gzipping the file\n");
	my $cmd = "gzip -f $output_file_v6";
	my $output = `$cmd`;
	
	close(INFILE1);
	close(INFILE2);
	
}


## Subroutines -------------------------------------------------------------
sub GetIPRange
{   
        my ($ip_address,$netmask,$row) = @_;
        my (@ip);
    
        ## for IPv4 type 
        if($ip_address =~ /(\d+)\.(\d+)\.(\d+)\.(\d+)/){
                ($ip[3],$ip[2],$ip[1],$ip[0]) = ($1,$2,$3,$4);
                if($netmask =~ /\d+\s*$/){
                        if($netmask < 0 || $netmask >32){
                                print LOG "Invalid mask (\"$netmask\") provided at line $row\n";
                                return (-1,-1);
                        }
			
			foreach (@ip){
				if( $_ > 255 || $_ < 0){
					print LOG "Invalid IPv4 address at line $row. Skipping the line.\n";
					return (-1,-1);
				}
			}
	
			my $ip_address_binary = inet_aton( $ip_address );
			my $netmask_binary    = ~pack("N", (2**(32-$netmask))-1);
#print "ip n mask :$ip_address $netmask $ip_address_binary n $netmask_binary\n";

			my $network_address    = inet_ntoa( $ip_address_binary & $netmask_binary );
			#my $first_valid        = inet_ntoa( pack( 'N', unpack('N', $ip_address_binary & $netmask_binary ) + 1 ));
			#my $last_valid         = inet_ntoa( pack( 'N', unpack('N', $ip_address_binary | ~$netmask_binary ) - 1 ));
			my $broadcast_address  = inet_ntoa( $ip_address_binary | ~$netmask_binary );
#print "$network_address $first_valid $last_valid $broadcast_address \n";

			return ($network_address,$broadcast_address);
                }

        }
	else{
		print LOG "Invalid IPv4 address format at line $row.\n";
	}
}

sub send_mail
{
        my ($from,$to,$cc,$subject,$type,$path,$filename) = @_;
        if(!defined $from || !defined $to ){
                print LOG "From/To field not defined,skipping mail sending.\n";
                return;
        }
        if(!defined $type){
                $type = "text/plain";
        }
        my $msg = MIME::Lite->new(
        From    => $from,
        To      => $to,
        Cc      => $cc,
        Subject => $subject,
        Type    => 'multipart/mixed',
        );

	$msg->attach(
		Type     => $type,
		Data => "GeoIP Database conversion done on".localtime."\n"
	);
        if(defined $path && defined $filename){
                if(-e "$path/$filename"){ #print "$filename file exists!!\n";
		close LOG;
                $msg->attach(
                	Type     => $type,
                	Path     => "$path/$filename",
                	Filename => $filename,
			Disposition => 'attachment'
                	);
                }
                else {
                        printlog("File \"$path\/$filename\" to be sent as attachment not found,sending mail without attachment.\n");
                        #return;
                }
        }

        $msg->send;

}

sub calculate_md5
{
	my $filename = shift;
	my ($digest,$digest2,$recent_file);

	my $str = localtime->strftime('%F');
	my $new_filename = $str."_".$filename;
	if($filename =~ /^Geo.*?zip/){
		printlog("\nRenaming the zipped file $filename to $new_filename");
		`mv $filename $new_filename`;
		$filename = $new_filename;
	}
	open(FILE, $filename);
        my $ctx = Digest::MD5->new;
       	$ctx->addfile(*FILE);
        my $digest = $ctx->hexdigest;
        close(FILE);

	my $zip_filename = "GeoIP_filenames.txt";
	my $ret = open FILE,">>$zip_filename";
	if($ret == 0){
                printlog("\nCouldn't open $zip_filename for writing.Skipping calculation of md5 hash.");
                return (1,$filename);
        }
	print FILE "$filename md5=$digest\n";
        close FILE;

	$ret = open(FILE,"<$zip_filename");
	if($ret == 0){
		printlog("\nCouldn't open $zip_filename for reading.Skipping calculation of md5 hash.");
		return (1,$filename);
	}
        my @list = <FILE>;
	if(scalar @list > 1){
	        $recent_file = $list[$#list-1]; 
		if($recent_file =~ /(.*?)\smd5\=(.*?)\n/){
                	$recent_file = $1;
                	$digest2 = $2;
			printlog("\nComparing md5 hash with the file : $recent_file");
        	}
	
        	close FILE;
		printlog("\nmd5 hash of file $filename is $digest.\nmd5 hash of file $recent_file is $digest2.");
	}else{
		printlog("\nNo file to compare the md5 hash.");
	}
	#my $out = `ls -rtl | grep '.*GeoLite2-City-CSV.zip'`;
	#my @list = split("\n",$out);
	#my $recent_file = ($list[$#list] =~ /\s+(.*?GeoLite2-City-CSV.zip)/);
	if($digest eq $digest2){
		return (0,$filename);
	}	
	return (1,$filename); 
	#if(-e "")
	#open FILE,"<GeoLite2-City-CSV_name.txt";	
	
}

sub exit_conversion
{
	printlog("\nExecution finished at ".localtime."\n");	
	exit;
}
sub printlog
{	my $msg = shift;
	my $stdout = $debug;
	print LOG $msg;
	if($stdout){
		print $msg;
	}
}

sub HELP
{
	my $file = $0;
	my $help = '
	Usage: perl '."$file".' [-opts]

	Download the csv format of the GeoIP db(https://dev.maxmind.com/geoip/geoip2/geolite2/) and extract the files in the same directory where this script is placed.
	Default action is to convert the files(GeoLite2-City-Blocks-IPv4.csv,GeoLite2-City-Blocks-IPv6.csv and GeoLite2-City-Locations.csv) present in current directory to netscaler format.
	The default file names are for Maxmind City based DB, if the user downloaded DB is GeoLite2-Country or others, the user should provide the input file accordingly.
	
	-b <filename> name of IPv4 block file to be converted. Default value: GeoLite2-City-Blocks-IPv4.csv
	-i <filename> name of IPv6 block file to be converted. Default value: GeoLite2-City-Blocks-IPv6.csv
	-l <filename> name of location file to be converted. Default value: GeoLite2-City-Locations.csv
	-o <filename> ipv4 output file. Default value: Netscaler_Maxmind_GeoIP_DB_IPv4.csv
	-p <filename> ipv6 output file. Default value: Netscaler_Maxmind_GeoIP_DB_IPv6.csv
	-logfile <filename> name of logfile	
	-debug prints all the messages to STDOUT
	';
	print "$help\n";
	exit 1;
}
