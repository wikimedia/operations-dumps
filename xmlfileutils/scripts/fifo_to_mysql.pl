#!/usr/bin/perl
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use IO::Uncompress::Bunzip2 qw(bunzip2 $Bunzip2Error);
use Fcntl;

require POSIX;

use strict;
use warnings;

use constant {
    BYTES   => 1,
    LINES   => 2
};

sub get_config_opts {
    return qw(db table sqlfile fifo mysqluser mysqlhost mysqlport mysqlpasswd passwdfile charset chunk mysqlopts);
}

sub get_config_flags {
    return qw(verbose);
}

sub get_cmdline_opts {
# add in opts that can be specified on command line but not in config file
    return (get_config_opts(), qw(configfile));
}

sub get_cmdline_flags {
# add in opts that can be specified on command line but not in config file
    return (get_config_flags(), qw(dryrun));
}

sub read_config_file {
    my $filename = shift(@_);
    my %opts_from_configfile = ();

    open (my $fh, "<", $filename) || die "failed to open cnfig file $filename: $!";
    while (my $line = <$fh>) {
	next if ($line =~ /^(#|\s*$)/);
	chomp($line);
	my($name, $value) = split(/ /, $line, 2);
	if (grep $name eq $_, get_config_opts()) {
	    $opts_from_configfile{$name} = $value;
	}
	elsif (grep $name eq $_, get_config_flags()) {
	    $opts_from_configfile{$name}++;
	}
	else {
	    print STDERR "bad line encountered in config file: $_\n";
	    usage();
	}
    }
    return \%opts_from_configfile;
}

sub merge_opts {
    my $defaults = shift(@_);
    my $opts_from_configfile = shift(@_);
    my $opts_from_cmdline = shift(@_);
    my %merged = ();

    foreach my $o (get_cmdline_opts(), get_cmdline_flags()) {
	if (exists($opts_from_cmdline->{$o})) {
	    $merged{$o} = $opts_from_cmdline->{$o};
	}
	elsif (exists($opts_from_configfile->{$o})) {
	    $merged{$o} = $opts_from_configfile->{$o};
	}
	else {
	    $merged{$o} = $defaults->{$o};
	}
    }
    return \%merged;
}

sub usage {
    my $usage_message = <<END;
Usage: $0 [--configfile filename] [-sqlfile filename] [--fifo filename]
          [--db dbname] [--table tablename] [--mysqlhost hostname] [--mysqlport portnum] 
          [--mysqluser username] [mysqlpasswd password] [--passwdfile filename]
          [--charset charsettype] [--chunk chunksize] [--mysqlopts option1,option2,...]
          [--help] [--dryrun] [--verbose]

Reads mysql table data formatted for LOAD DATA INFILE from specified sql file
and feeds it to a named pipe in chunks; for each chunk it invokes mysql to load
the data, and when finished removes the pipe

The file for loading into mysql should have been formatted as follows:
  fields are separated by tabs
  double quotes and single quotes embedded in strings are escaped with backslash (\\)
  newlines embedded in strings are converted to \\n
  carriage returns embedded in strings are converted to \\r
  control-z embedded in strings are converted to \\Z
  nulls embedded in strings are converted to \\N
  tabs embedded in strings are converted to \\t
  strings are enclosed in double quotes (\")
  scalars are not enclosed by any character

Options:

--config       Name of config file, options provided here will be overriden
               by options specified on the command line
               File format: blank lines or lines beginning with # are ignored
               All other lines must contain the option name, one or more spaces, option value
               The following may be specified: db, table, sqlfile, fifo, chunk, mysqlhost,
               mysqlport, mysqluser, mysqlpasswd, charset, mysqlopts, verbose
--sqlfile      Name of the file of sql data to be read in; if the name ends
               in .gz or .bz2 it will be zcatted or bzcatted accordingly
--fifo         Name of the fifo to be created (it will be removed later)
               default: /tmp/mysql_fifo
--db           Name of the database into which to load the data
--table        Name of the specific table into which to load the data
--mysqlhost    Hostname of mysql server
               default: 127.0.0.1
--mysqlport    Port number for mysql server
               default: 3306
--mysqluser    Username for mysql access
               default: root
--mysqlpasswd  Password for mysql access
               If you need to supply the empty password you must omit this as well as the
               passwdfile option and provide a blank password when prompted
               default: none (in which case the user will be prompted for a password)
               which will be passed to mysql on the command line, a security risk
--passwdfile   Filename for reading in password of user for mysql access; if specified
               this overrides the mysqlpasswd option
	       The file should contain the following:
	         [client]
	         password=passwordvaluehere
--charset      Character set for LOAD DATA INFILE
               default: binary (no conversion done on input)
--chunk        Number of bytes or lines of content after which the next line of input
               will be sent in a separate chunk; note that if your data has
               a maximum row size of X bytes then specifying Y bytes for the chunk
               size may wind up sending X + Y - 1 bytes in a chunk in the worst case
               If the argument ends in 'l' it's the number of lines, otherwise it's
               the number of bytes.
               default: 100 million bytes
--mysqlopts    Comma-separated list of options to turn off before starting
               the LOAD DATA INFILE transaction; any options specified here will be passed
               in as commands of the type 'SET option=0' before the LOAD DATA command is given
               Some things you may want to turn off depending on your tolerance to risk:
                 unique_checks, foreign_key_checks, sql_log_bin
               default: don't turn anything off

Flags:

--help         Show this usage message
--dryrun       Create the fifo and open the sql file but don't actually run the mysql
               client or write to the fifo, do everything else
               default: actually do the commands
--verbose      Display information about what is being done; if passed more than once,
               verbose level is increased
               default: quiet mode
END
    print STDERR $usage_message;
    exit(1);
}

sub check_mandatory_opts {
    my $o = shift(@_);
    while (my $name = shift(@_)) {
	if ($o->{$name} eq "") {
	    print STDERR "missing mandatory option '$o'\n";
	    usage();
	}
    }
}

sub check_alphanumeric_opts {
    # well, alphanumeric plus a couple things :-P
    my $o = shift(@_);
    while (my $name = shift(@_)) {
	if ($o->{$name} !~ /^[a-zA-Z\-_,]*$/) {
	    print STDERR "'$o->{$name}' must be alphanumeric\n";
	    usage();
	}
    }
}

sub check_notempty_opts {
    my $o = shift(@_);
    while (my $name = shift(@_)) {
	if ($o->{$name} eq "") {
	    print STDERR "'$o->{$name}' must not be an empty value\n";
	    usage();
	}
    }
}

sub check_numeric_opts {
    my $o = shift(@_);
    while (my $name = shift(@_)) {
	if ($o->{$name} !~ /^\d+$/) {
	    print STDERR "'$o->{$name}' must be a number\n";
	    usage();
	}
    }
}


sub cleanup {
    my $path = shift(@_);
    my $fh = shift(@_);
    unlink($path) if (-e $path);
    close($fh) if (fileno($fh));
}

sub wait_and_bail {
    my $pid = shift(@_);
    my $fifo = shift(@_);
    my $sqlfh = shift(@_);

    my $result = kill('KILL', $pid);
    $result = waitpid($pid, 0);
    my $exit_code = ${^CHILD_ERROR_NATIVE};
    print STDERR "result from fork of mysql was $result, with exit code from command of $exit_code\n";
    cleanup($fifo, $sqlfh);
    exit(-1);
}

sub set_empty {
    my $h = shift(@_);
    while (my $name = shift(@_)) {
	$h->{$name} = "";
    }
}

sub set_default_opts {
    my %defaults = ();
    set_empty(\%defaults, qw(db table sqlfile mysqlopts mysqlpasswd passwdfile));
    $defaults{'fifo'} = "/tmp/mysql_fifo";
    $defaults{'mysqlhost'} = "127.0.0.1";
    $defaults{'mysqlport'} = "3306";
    $defaults{'mysqluser'} = "root";
    $defaults{'chunk'} = "100000000b";
    $defaults{'charset'} = "binary";
    $defaults{'verbose'} = 0;
    $defaults{'dryrun'} = 0;
    return \%defaults;
}

sub process_opts {
    my $defaults = set_default_opts();
    my $configfile = "";

    my %opts_from_cmdline = ();
    my $opts_from_config = {};

    while (my $opt = shift(@_)) {
	if ($opt =~ /^--(.+)$/) {
	    $opt = $1;
	}
	else {
	    print STDERR "Badly formed option '$opt'\n";
	    usage();
	}
	if ($opt eq "help") {
	    print STDERR "Help displayed\n";
	    usage();
	}
	elsif (grep $opt eq $_, get_cmdline_opts()) {
	    $opts_from_cmdline{$opt} = shift(@_);
	}
	elsif (grep $opt eq $_, get_cmdline_flags()) {
	    $opts_from_cmdline{$opt}++;
	}
	else { usage(); }
    }
    if (exists $opts_from_cmdline{'configfile'}) {
	$opts_from_config = read_config_file($opts_from_cmdline{'configfile'});
    }
    my $o = merge_opts($defaults, $opts_from_config, \%opts_from_cmdline);
    return $o;
}

sub check_opts {
    my $o = shift(@_);

    check_mandatory_opts($o, qw(db table sqlfile));
    check_alphanumeric_opts($o, qw(db table charset mysqluser mysqlopts));
    check_notempty_opts($o, qw(charset mysqluser));
    check_numeric_opts($o, qw(mysqlport));

    if ($o->{'mysqlhost'} !~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $o->{'mysqlhost'} !~ /^[a-z0-9\.\-]+$/ && $o->{'mysqlhost'} !~ /^[a-fA-F0-9:]+$/) {
	print STDERR "'mysqlhost' must be a hostname or an ip address if specified\n";
	usage();
    }
}

sub get_units {
    my $o = shift(@_);

    my $units = BYTES;
    if (substr($o->{'chunk'}, -1, 1) eq "l") {
	chop($o->{'chunk'});
	$units = LINES;
    }
    elsif (substr($o->{'chunk'}, -1, 1) eq "b") {
	chop($o->{'chunk'});
    }
    return $units;
}

sub get_mysql_passwd {
    my $o = shift(@_);
    my $mysqlpasswd = "";

    if ($o->{'passwdfile'}) { return "" }

    if ($o->{'mysqlpasswd'} eq "") {
	print STDERR "Password for mysql user needed: ";
	$mysqlpasswd = <STDIN>;
	chomp($mysqlpasswd);
    }
    else {
	$mysqlpasswd = $o->{'mysqlpasswd'};
    }
    return $mysqlpasswd;
}

sub get_mysql_cmd {
    my $o = shift(@_);
    my $mysqlpasswd = shift(@_);
    my $n;

    my @opts = split(/,/, $o->{'mysqlopts'});
    my $begin_opts = join(';', map { "SET $_=0" } @opts);
    if ($begin_opts) { $begin_opts = $begin_opts . ";"; }

    my $format = "FIELDS OPTIONALLY ENCLOSED BY '\\\''";
    
    my $commit = "";

    # need this because otherwise the whole load data will just be rolled back at client close :-P
    $commit = "COMMIT;" if ($begin_opts =~ /autocommit/);

    my @passwdfile_opts = ();
    if ($o->{'passwdfile'}) { @passwdfile_opts = ("--defaults-extra-file=$o->{'passwdfile'}"); }
    my @passwd_opts = ();
    if ((! @passwdfile_opts) && $mysqlpasswd) { @passwd_opts = ("-p" . $mysqlpasswd); }

    my @command = ("mysql", @passwdfile_opts, "-u", $o->{'mysqluser'}, @passwd_opts, "-e", "use $o->{'db'}; $begin_opts LOAD DATA INFILE '$o->{'fifo'}' INTO TABLE $o->{'table'} CHARACTER SET $o->{'charset'} $format ; $commit");
    return \@command;
}

sub fifo_to_mysql {
    my $o = process_opts(@ARGV);

    if ($o->{'chunk'} !~ /^\d+[lb]?$/) {
	print STDERR "chunk size $o->{'chunk'} must be a number followed by optional l or b\n";
	usage();
    }

    check_opts($o);
    my $units = get_units($o);
    my $mysqlpasswd = get_mysql_passwd($o);
    my $command = get_mysql_cmd($o, $mysqlpasswd);

    print STDERR "*** Dry run only, no msyql commands will be run\n" if ($o->{'dryrun'});

    POSIX::mkfifo($o->{'fifo'}, 0666) or die "can't mknod $o->{'fifo'}: $!";
    print STDERR "INFO: fifo $o->{'fifo'} created\n" if ($o->{'verbose'});

    my $sqlfh;

    if ($o->{'sqlfile'} =~ /\.gz$/) {
	$sqlfh = new IO::Uncompress::Gunzip $o->{'sqlfile'} or die "zip failed: $GunzipError\n" ;
    }
    elsif ($o->{'sqlfile'} =~ /\.bz2$/) {
	$sqlfh = new IO::Uncompress::Bunzip2 $o->{'sqlfile'} or die "zip failed: $Bunzip2Error\n" ;
    }
    else {
	open($sqlfh, "<", $o->{'sqlfile'});
    }

    print STDERR "INFO: sql file opened\n" if ($o->{'verbose'});

    my $pid;

    while (! eof($sqlfh)) {
	print STDERR "INFO: " . ($o->{'dryrun'} ? "would write" : "writing") . " chunk\n" if ($o->{'verbose'});
	if (! $o->{'dryrun'}) {
	    defined($pid = fork()) or die "Can't fork: $!";
	    if (!$pid) {
		exec(@$command) or die "exec of mysql failed\n";
	    }
	}
	print STDERR "INFO: " . ($o->{'dryrun'} ? "would run " : "runnning ") . join(' ', @$command). "\n" if ($o->{'verbose'} > 1);
	if (! $o->{'dryrun'}) {
	    # first check the forked process is actually running, else the open will hang...
	    my $exists = kill 0, $pid;
	    if ($exists) {
		my $TIMEOUT_IN_SECONDS = 5;
		eval {
		    local $SIG{ALRM} = sub { die "alarm\n" };
		    alarm($TIMEOUT_IN_SECONDS);
		    sysopen(FIFO, $o->{'fifo'}, O_WRONLY) or die "can't write $o->{'fifo'}: $!";
		    alarm(0);
		};
		if ($@) {
		    print STDERR "timeout trying to open fifo, giving up\n";
		    wait_and_bail($pid, $o->{'fifo'}, $sqlfh);
		}
	    }
	    else {
		print STDERR "mysql process exited early, giving up\n";
		wait_and_bail($pid, $o->{'fifo'}, $sqlfh);
	    }
	}
	my $written = 0;
	while ($written < $o->{'chunk'} && ! eof($sqlfh)) {
	    my $line = <$sqlfh>;
	    if (! $o->{'dryrun'}) { print FIFO $line; }
	    if ($units == BYTES) { $written += length($line); }
	    else { $written++; } # LINES
	}
	if (! $o->{'dryrun'}) { close FIFO; }
	if (! $o->{'dryrun'}) {
	    my $result = waitpid($pid, 0);
	    my $exit_code = ${^CHILD_ERROR_NATIVE};
	    print STDERR "result from fork of mysql is $result, with exit code from command of $exit_code\n" if ($o->{'verbose'});
	    if ($exit_code) {
		print STDERR "error returned from mysql, bailing\n";
		cleanup($o->{'fifo'}, $sqlfh);
		exit(-1);
	    }
	}
    }

    print STDERR "INFO: cleaning up\n" if ($o->{'verbose'});
    cleanup($o->{'fifo'}, $sqlfh);
}

fifo_to_mysql();
exit(0);
