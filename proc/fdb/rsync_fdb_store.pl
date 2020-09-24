#!/usr/local/bin/perl -w
#
# Copyright (c) SMIS-Andry 1999-2010. All rights reserved.
#
# Utilite for efficiently mirroring local FDB file storage to remote host
# using rsync program with rsh transport.
# It allows sync only those files, that located in subdirectories, modyfied since last utilite run
# /data/fdb_store/<db>/<table>/<file1>/00001,00002, .. /{files}
#
# 6/4/2010  01:50:18

use strict;

#use File::Path;
use Getopt::Long;
use Data::Dumper;

use SDB::common;
use SDB::mysql_object;
use SDB::datetime qw(datetime2c_time time2datetime seconds2age);
use SDB::hash2config;

#use SDB::file;
#use SDB::time;
#use SDB::task_lock;

### Subroutines
sub readfile($);
sub writefile($$);

### Constants
my $DEFAULT_CONFIG_FILE   = "fdb.conf";
my $DEFAULT_RSYNC_OPTIONS = "-e rsh -aR";
my $RSYNC                 = "rsync";
my $SUBDIR_MASK           = '\d\d\d\d\d';

### Flags
my $DEBUG     = 1;
my $SQL_DEBUG = 0;
my $FORCE     = 0;
my $DUMMY = 0;

# Command line processing
my $usage = qq(
$0 [-config <file>] [-force] [-rsync_opts "<opts>"] [-first_subdir <subdir>] [-last_subdir <subdir>] [-dummy]
        [-file_fields field1,..] <db> <table> <remote_path>
    -config <file> - path to config file (by default "./fdb.conf" )
    -force - syncync all files regardless subdirectories modification times
    -rsync_opts - options for rsync program run (by default "$DEFAULT_RSYNC_OPTIONS")
    -first_subdir <subdir> - sync from specified subdir
    -last_subdir <subdir> - don\'t sync subdirectories after specified subdir
    -dummy - no real syncing, just show commands
    -file_fields - list of FDB file fields
    <db> - name of FDB database
    <remote_path> - remote host and path in standart rcp notation "[user@]host:path"
        can be specified absolute path to directory also
);

my $config_file = $DEFAULT_CONFIG_FILE;
my $rsync_opts  = $DEFAULT_RSYNC_OPTIONS;
my( $first_subdir, $last_subdir, $ff );
check( GetOptions(
    "config=s" => \$config_file,
    "rsync_opts=s" => \$rsync_opts,
    "first_subdir=s" => \$first_subdir,
    "last_subdir=s" => \$last_subdir,
    "file_fields=s" => \$ff,
    "dummy!" => \$DUMMY,
    "force!" => \$FORCE ),
$usage );

check( -f $config_file, "Config file \"$config_file\" not found" );
check( @ARGV == 3,      $usage );
my @specified_file_fields;
if( $ff ) {
    @specified_file_fields = split( /,/, $ff );
}

print STDERR "\n*** Force mode ***" if $FORCE;
print STDERR "\n*** DUMMY mode ***" if $DUMMY;
print STDERR "\n*** First subdir $first_subdir ***" if $first_subdir;
print STDERR "\n*** Last subdir $last_subdir ***" if $last_subdir;
print STDERR "\n*** Specified file fields: ", join( ",", @specified_file_fields )," ***" if @specified_file_fields;

my ( $db, $table, $remote_path ) = @ARGV;
my ( $user, $host, $path );
if( $remote_path =~ /^\// ) {
    $user = "hulio";
    $host = "localhost";
    $path = $remote_path;
} else {
    check( $remote_path =~ /^(\w+\@)?([\w\d\.]+)\:([\/\w]+)$/, "Illegal \"remote_path\" parameter \"$remote_path\"" );
    ( $user, $host, $path ) = ( $1, $2, $3 );
}

# Parse config file
my %cfg;
tie %cfg, 'SDB::hash2config', $config_file,
  {
    required        => [qw( common::FILE_STORAGE_ROOT_DIR MYSQL_HOST MYSQL_USER MYSQL_PASSWORD )],
    default_section => "mysql",
  };

my $storage_dir = $cfg{'common::FILE_STORAGE_ROOT_DIR'} . "/fdb_store";
check( -d $storage_dir, "Directory \"$storage_dir\" doesn't exist" );

my $db_storage_dir = $storage_dir . "/" . $db;
check( -d $db_storage_dir, "DB storage dir \"$db_storage_dir\" not found" );
my $table_storage_dir = $db_storage_dir . "/" . $table;
check( -d $table_storage_dir, "Table storage dir \"$table_storage_dir\" not found" );

# Connect to DB
my $mysql = new SDB::mysql_object(
    host      => $cfg{MYSQL_HOST},
    db        => $db,
    user      => $cfg{MYSQL_USER},
    password  => $cfg{MYSQL_PASSWORD},
    sql_debug => $SQL_DEBUG,
);

# Get table info
my $table_info  = $mysql->get_table_info($table);
my @fields      = @{ $table_info->{fields} };
my @file_fields = map { /^(\w+)_/; $1 } grep( /_NAME$/, @fields );
if( @specified_file_fields ) {
    my @arr;
    foreach ( @specified_file_fields ) {
        check( find_in_list( \@file_fields, $_), "Illegal file field \"$_\"" );
        push @arr, $_;
    }
    @file_fields = @arr;
}


# Obtain current time
my $current_run_dt = time2datetime( time() );

# Look for utilite last run time file
my $last_run_time;
my $extra_info = "";
if( $first_subdir ) {
    $extra_info .= "-first_subdir$first_subdir";
}
if( $last_subdir ) {
    $extra_info .= "-last_subdir$last_subdir";
}

my $last_run_time_file = sprintf( "/tmp/fdbsync_%s-%s-%s%s.time", $db, $table, $host, $extra_info );
if ( -f $last_run_time_file ) {
    my $last_run_dt = readfile($last_run_time_file);
    print STDERR "\n### Synchronize subdirectories changed after \"$last_run_dt\"";
    check( $last_run_dt =~ /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/, "Illegal datetime \"$last_run_dt\"" );
    $last_run_time = datetime2c_time($last_run_dt);
}
else {
    print STDERR "\n### Last run file not found. Synchronize all subdirectories!!!";
}

my %subdirs;
my %times;
my $is_resultive = 0;
foreach my $ff (@file_fields) {
    my $ff_dir = $table_storage_dir . "/" . $ff;
    $subdirs{$ff} = 0;
    $times{$ff}   = 0;

    # Obtain list of subdirs to sync
    check( opendir( DIR, $ff_dir ), $! );
    check( chdir($ff_dir), "$!" );
    my @subdirs;
    if ( $FORCE || !defined $last_run_time ) {
        @subdirs = grep { -d && !-l && /^$SUBDIR_MASK$/ } readdir(DIR);
    }
    else {
        @subdirs = grep { -d && !-l && /^$SUBDIR_MASK$/ && (stat)[9] > $last_run_time } readdir(DIR);
    }
    closedir(DIR);
    if( defined $first_subdir ) {
        @subdirs = grep( $_ ge $first_subdir, @subdirs );
    }
    if( defined $last_subdir ) {
        @subdirs = grep( $_ le $last_subdir, @subdirs );
    }

    my @arr;

    print STDERR "\n> Process file field \"$ff\": ";
    unless (@subdirs) {
        print STDERR "\nNo subdirectories are changed";
        next;
    }

    print STDERR "\nChanged ", scalar(@subdirs), " subdirectories";

    $is_resultive = 1;

    check( chdir($storage_dir), "$!" );
    foreach my $subdir ( sort @subdirs ) {
        $subdirs{$ff}++;
        my $start_time = time();
        my $cmd        = "$RSYNC $rsync_opts $db/$table/$ff/$subdir $remote_path";
        print STDERR "\n$subdirs{$ff}> $cmd .. " if $DEBUG;
        unless( $DUMMY ) {
            my $ret = safe_system($cmd);
            check( $ret == 0, $! );
            my $finish_time = time();
            my $duration    = seconds2age( $finish_time - $start_time );
            print STDERR "Ok ($duration)" if $DEBUG;
            $times{$ff} += ( $finish_time - $start_time );
        }
    }

    #	print STDERR "\n### Processed ", scalar(@subdirs), " subdirectories";
}

# Write last run time file
unless( $DUMMY ) {
    writefile( $last_run_time_file, $current_run_dt );
}

if ($is_resultive) {
    do_exit( $RET_OK, "*** Synchronization was done successfully ***" );
}
else {
    do_exit( $RET_NODATA, "*** Nothing was done ***" );
}

#print STDERR "\n### Execution statistics: ";
#
#foreach my $ff ( keys %subdirs ) {
#	print STDERR "\nFile field - $ff, synchronized subdirectories - $subdirs{$ff}, total time - ", seconds2age($times{$ff});
#}

### Subroutines
sub readfile ($) {
    my $file = shift;
    my $buffer;

    check( open( FILE, "<$file" ), "Cannot open file \"$file\" for reading" );
    $buffer = join( "", <FILE> );
    close(FILE);
    return $buffer;
}

sub writefile ($$) {
    my $file   = shift;
    my $buffer = shift;

    check( open( FILE, ">$file" ), "Cannot open file \"$file\" for writing" );
    print FILE $buffer;
    close(FILE);
}
