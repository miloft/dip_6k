#!/usr/local/bin/perl -w
#
# Copyright (c) SMIS-Andry 1999-2006. All rights reserved.
#
# 3/30/2006 15:50

use strict;
use SDB::common;
use SDB::file;
use SDB::time;
use SDB::task_lock;
use SDB::mysql;
use SDB::hash2config;
use File::Path;
use Getopt::Long;

use Data::Dumper;

### Constants
my $DEFAULT_CONFIG_FILE = "fdb.conf";

### Flags
my $DEBUG = 0;
my $SQL_DEBUG = 0;

# Command line processing
my $usage = qq(
Usage 1: Check files in volume "ONLINE"
$0 [-config <file>] <db> <table> <file_field>
\n
Usage 2: Check files in specified volume
$0 [-config <file>] <db> <table> <file_field> <volume> <off-line storage dir>
	default config file "./fdb.conf"
);
my $config_file = $DEFAULT_CONFIG_FILE;
check( GetOptions( "config=s" =>\$config_file ), $usage );
check( @ARGV == 3 || @ARGV == 5, $usage );

my( $db, $table, $file_field, $volume, $offline_storage_dir ) = @ARGV;

# Parse config file
my %cfg;
tie %cfg, 'SDB::hash2config', $config_file, {
	required => [ qw( common::FILE_STORAGE_ROOT_DIR MYSQL_HOST MYSQL_USER MYSQL_PASSWORD ) ],
	default_section => "mysql",
};

my $storage_dir;
if( defined $volume ) {
	$storage_dir = $offline_storage_dir."/".$volume;
} else { # check ONLINE volume
	$volume = "ONLINE";
	$storage_dir = $cfg{'common::FILE_STORAGE_ROOT_DIR'}."/fdb_store";
}
check( -d $storage_dir, "Directory \"$storage_dir\" doesn't exist" );

# Connect to DB
dbi_connect( "database=$db;host=$cfg{MYSQL_HOST}", $cfg{MYSQL_USER}, $cfg{MYSQL_PASSWORD} );

# Get table info
my $table_info = get_table_info( $table );
my @fields = @{$table_info->{fields}};
my @file_fields = map{ /^(\w+)_/;$1  } grep( /_NAME$/, @fields );
check( find_in_list( \@file_fields, $file_field ),
	"Illegal file field name \"$file_field\". Existing file fields: ".join( ", ", @file_fields ) );

# Query database
my $file_fields_list = join( ",", map{ $file_field."_".$_ } qw( SIZE STORAGEFILE VOLUME NAME ) );


my $sql = "SELECT ID,$file_fields_list FROM $table WHERE $file_field"."_VOLUME"."='$volume' ORDER by ID";
print STDERR "\n> $sql" if $SQL_DEBUG;
my $sth = mysql_execute( $sql , {mysql_use_result => 1 });
my $missing_count = 0;
my $count = 0;
while( my $href = $sth->fetchrow_hashref() ) {
	$count++;
	my $file = "$storage_dir/$db/$table/$file_field/".$href->{$file_field."_STORAGEFILE"};
	print STDERR "\ncheck file: $file" if $DEBUG;

	my $err_type;
	if( ! -f $file ) {
		$err_type = "not found";
	} elsif( (-s $file) != $href->{$file_field."_SIZE"} ) {
		$err_type = "illegal size";
	} else {
		next;
	}

	$missing_count++;
	print join( "|", $href->{ID}, map{$href->{$file_field."_$_"}} qw( NAME VOLUME SIZE STORAGEFILE ) ), " - $err_type", "\n";
}
$sth->finish();

print STDERR "\n$count records processed";
if( $missing_count ) {
	do_exit( $RET_ERR, "$missing_count files not found or have illegal size" );
} else {
	do_exit( $RET_OK, "Check completed successfully" );
}
