#!/usr/bin/perl

=head1 Name

qsub-sge.pl -- control processes running on linux SGE system

=head1 Description

This program throw the jobs and control them running on linux SGE system. It reads jobs 
from an input shell file. One line is the smallest unit of  a single job, however, you can also specify the 
number of lines to form a single job. For sequential commands, you'd better put them
onto a single line, seperated by semicolon. In anywhere, "&" will be removed 
automatically. The program will terminate when all its jobs are perfectly finished. 

If you have so many jobs, the efficency depends on how many CPUs you can get,
or which queque you have chosen by --queue option. You can use the --maxjob option to 
limit the number of throwing jobs, in order to leave some CPUs for other people. 
When each job consumes long time, you can use the --interval option to increase interval
time for qstat checking , in order to reduce the burden of the head node.

As SGE can only recognize absolute path, so you'd better use absolute path everywhere,
we have developed several ways to deal with path problems:
(1) We have added a function that converting local path to absolute
path automatically. If you like writting absolute path by yourself, then you'd better close this
function by setting "--convert no" option. 
(2) Note that for local path, you'd better write
"./me.txt" instead of only "me.txt", because "/" is the  key mark to distinguish path with
other parameters.  
(3) If an existed file "me.txt" is put in front of the redirect character ">", 
or an un-created file "out.txt" after the redirect character ">", 
the program will add a path "./" to the file automatically. This will avoid much
of the problems which caused by forgetting to write "./" before file name. 
However, I still advise you to write "./me.txt" instead of just "me.txt", this is a good habit.
(4) Please also note that for the re-direct character ">" and "2>", there must be space characters 
both at before and after, this is another good habit.

There are several mechanisms to make sure that all the jobs have been perfectly finished:
(1) We add an auto job completiton mark "This-Work-is-Completed!" to the end of the job, and check it after the job finished
(2) We check "GLIBCXX_3.4.9 not found" to make sure that the C/C++ libary on computing nodes are in good state
(3) We provide a "--secure" option to allow the users define their own job completition mark. You can print a mark
    (for example, "my job complete") to STDERR at the end of your program, and set --secure "my job complete" at 
	this program. You'd better do this when you are not sure about wheter there is bug in your program.
(4) We provide a "--reqsub" option, to throw the unfinished jobs automatically, until all the jobs are 
    really finished. By default, this option is closed, please set it forcely when needed. The maximum 
	reqsub cycle number allowed is 1000.
(5) Add a function to detect the died computing nodes automatically.
(6) Add checking "iprscan: failed" for iprscan
(7) Add a function to detect queue status, only "r", "t", and "qw" is considered correct. 
(8) Add check "failed receiving gdi request"

Normally, The result of this program contains 3 parts: (Note that the number 24137 is the process Id of this program)
(1) work.sh.24137.globle,     store the shell scripts which has been converted to global path 
(2) work.sh.24137.qsub,       store the middle works, such as job script, job STOUT result, and job STDERR result
(3) work.sh.24137.log,      store the error job list, which has been throwed more than one times.

I advice you to always use the --reqsub option and check the .log file after this program is finished. If you find "All jobs finished!", then
then all the jobs have been completed. The other records are the job list failed in each throwing cycle, but
don't worry, they are also completed if you have used --reqsub option.

For the resource requirement, by default, the --resource option is set to vf=1.9G, which means the total
memory restriction of one job is 1.9G. By this way, you can throw 8 jobs in one computing node, because the 
total memory restriction of one computing node is 15.5G. If your job exceeds the maximum memory allowed,
then it will be killed forcely. For large jobs, you must specify the --resource option manually, which 
has the same format with "qsub -l" option. If you have many small jobs, and want them to run faster, you
also need to specify a smaller memory requirement, then more jobs will be run at the same time. The key
point is that, you should always consider the memory usage of your program, in order to improve the efficency
of the whole cluster.

line starts with # would be skip,  but line count should contain this line.

=head1 Version

  Author: Fan Wei, fanw@genomics.org.cn
  Autor: Hu Yujie  huyj@genomics.org.cn
  Version: 8.1,  Date: 2008-10-30

=head1 Usage
  
  perl qsub-sge.pl <jobs.txt>
  --queue <str>     specify the queue to use, default no
  --interval <num>  set interval time of checking by qstat, default 3 seconds
  --lines <num>     set number of lines to form a job, default 1
  --maxjob <num>    set the maximum number of jobs to throw out, default 30
  --convert <yes/no>   convert local path to absolute path, default yes  
  --secure <mark>   set the user defined job completition mark, default no need
  --reqsub          reqsub the unfinished jobs untill they are finished, default no       
  --continue        continue running unfinished job and skip finished job , default no       
  --resource <str>  set the required resource used in qsub -l option, default vf=0.9G
  --jobprefix <str> set the prefix tag for qsubed jobs, default work
  --quota           analysis dir disk quota ,default is "100000000000000000G" 
  --analysis        analysis dir pathway , default is shell/../../..
  --nodu            do not check analysis dir size 
  --maxcycle        max cycle ,default 10 
  --verbose         output verbose information to screen   
  --help            output help information to screen  

=head1 Exmple
  
  1.work with default options (the most simplest way)
  perl qsub-sge.pl ./work.sh

  2.work with user specifed options: (to select queue, set checking interval time, set number of lines in each job, and set number of maxmimun running jobs)
  perl qsub-sge.pl --queue all.q -interval 1 -lines 3 -maxjob 10  ./work.sh

  3.do not convert path because it is already absolute path (Note that errors may happen when convert local path to absolute path automatically)
  perl qsub-sge.pl --convert no ./work.sh

  4.add user defined job completion mark (this can make sure that your program has executed to its last sentence)
  perl qsub-sge.pl -inter 1  -secure "my job finish" ./work.sh

  5.reqsub the unfinished jobs until all jobs are really completed (the maximum allowed reqsub cycle is 10)
  perl qsub-sge.pl --reqsub ./work.sh

  6.work with user defined memory usage
  perl qsub-sge.pl --resource vf=1.9G ./work.sh

  7.recommend combination of usages for common applications (I think this will suit for 99% of all your work)
  perl qsub-sge.pl --queue all.q --resource vf=1.9G -maxjob 10 --reqsub ./work.sh

=cut


use strict;
use Getopt::Long;
use FindBin qw($Bin $Script);
use File::Basename qw(basename dirname); 
use Data::Dumper;

##get options from command line into variables and set default values
my ($Queue, $Interval, $Lines, $Maxjob, $Convert,$Secure,$Reqsub,$Resource,$Job_prefix,$Verbose, $Help , $Continue);
my ($quota , $Analysis_dir , $max_cycle , $nodu); 
GetOptions(
	"lines:i"=>\$Lines,
	"maxjob:i"=>\$Maxjob,
	"interval:i"=>\$Interval,
	"queue:s"=>\$Queue,
	"convert:s"=>\$Convert,
	"secure:s"=>\$Secure,
	"reqsub"=>\$Reqsub,
	"continue" => \$Continue,
	"resource:s"=>\$Resource,
	"jobprefix:s"=>\$Job_prefix,
	"quota:s" => \$quota,
	"analysis:s" => \$Analysis_dir,
	"nodu"  => \$nodu , 
	"maxcycle:i" => \$max_cycle,
	"verbose"=>\$Verbose,
	"help"=>\$Help
);
$Queue ||= "sci.q";
$Interval ||= 200;
$Lines ||= 1;
$Maxjob ||= 30;
$Convert ||= 'yes';
$Resource ||= "vf=0.9G";
$Job_prefix ||= "work";
$quota ||= "100000000000000000G";
$max_cycle ||= 10 ;
die `pod2text $0` if (@ARGV == 0 || $Help);

my $work_shell_file = shift;
$work_shell_file = `readlink -f $work_shell_file`;
chomp($work_shell_file);
#print "$work_shell_file\n";

##global variables
my $work_shell_file_globle = $work_shell_file.".$$.globle";
my $work_shell_file_error = $work_shell_file.".$$.log";
my $Work_dir = $work_shell_file.".$$.qsub";
my $current_dir = `pwd`; chomp $current_dir;
#my $abs_work_dir = `readlink -f $Work_dir` ;
#chomp($abs_work_dir);
$Analysis_dir ||= "$Work_dir/../../../";
#print "aaaa$Analysis_dir\taaaa";die; 
if ($Convert =~ /y/i) {
	absolute_path($work_shell_file,$work_shell_file_globle);
}else{
	$work_shell_file_globle = $work_shell_file;
}

## read from input file, make the qsub shell files
my $line_mark = 0;
my $Job_mark="00000";
mkdir($Work_dir);
my @Shell;  ## store the file names of qsub sell
open IN, $work_shell_file_globle || die "fail open $work_shell_file_globle";
my %finish_blocks; ##### store finish jobs , and not running this jobs  00001 ==> 1 

readPreLogs($work_shell_file , \%finish_blocks ) if (defined $Continue); ## read log file and store finish job 

##### read shell file and split it  
while(<IN>){
	chomp;
	s/&/;/g;
	next unless($_);
	if (/^\s*#/){
		$line_mark++;
		next;
	}
	if ($line_mark % $Lines == 0) {
		$Job_mark++;
		if (not exists($finish_blocks{$Job_mark})){
			open OUT,">$Work_dir/$Job_prefix\_$Job_mark.sh" || die "failed creat $Job_prefix\_$Job_mark.sh";
			push @Shell,"$Job_prefix\_$Job_mark.sh";
		}else{

		}
	}
	s/;\s*$//;  ##delete the last character ";", because two ";;" characters will cause error in qsub
	s/;\s*;/;/g;
	#print OUT $_."&& echo This-Work-is-Completed!\n";
	print OUT $_." &&  " if (not exists($finish_blocks{$Job_mark})) ;

	if (not exists($finish_blocks{$Job_mark})) { 
		if ($line_mark % $Lines == $Lines - 1) {
			print OUT " echo [Job:$Job_mark] This-Work-is-Completed!\n";
			close OUT;
		}
	}
	$line_mark++;
}
close IN;
close OUT;
############ return @Shell

print STDERR "make the qsub shell files done\n" if($Verbose);


## run jobs by qsub, until all the jobs are really finished
my $qsub_cycle = 1;
my %stat_p;
my $total_job_number = @Shell;
my %all_cycle_job ;  ### jobid ==> RD0208-3MergeFq_00110.sh 

open OUT, ">>$work_shell_file_error" || die "fail create $$work_shell_file_error";

my$old_fh = select(OUT);
$| = 1 ;
select($old_fh);

print OUT "DISK_QUOTA\t$quota\n";
print OUT "Max_Jobs\t$Maxjob\n";
close OUT ; 

while (@Shell) {
	#print "start $qsub_cycle\n";
	## throw jobs by qsub
	##we think the jobs on died nodes are unfinished jobs
	open OUT, ">>$work_shell_file_error" || die "fail create $$work_shell_file_error";
	
	my$old_fh = select(OUT);
	$| = 1 ;
	select($old_fh);

	my %Alljob; ## store all the job IDs of this cycle
	my %Runjob; ## store the real running job IDs of this cycle
	my %Error;  ## store the unfinished jobs of this cycle
	chdir($Work_dir); ##enter into the qsub working directoy
	my $job_cmd = "qsub -cwd -S /bin/sh ";  ## -l h_vmem=16G,s_core=8 
	#my $job_cmd = "qsub -cwd -l h=c0001  " ;#-S /bin/sh ";  ## -l h_vmem=16G,s_core=8 
	$job_cmd .= "-q $Queue "  if(defined $Queue); ##set queue
    $job_cmd .= "-l $Resource " if(defined $Resource); ##set resource
#	warn $job_cmd;
	my $finish_num = 0 ;
	my $flag_hold_output = 0 ; 
	my $flag_is_too_full = 0 ; 
	### first , qsub all job on node in the first round
	for (my $i=0; $i<@Shell; $i++) {
		while (1) {
			$quota = readPreLogs($work_shell_file , 0 , $quota);
			if ( not defined $nodu) { 
				$flag_is_too_full = is_too_full($quota , $Analysis_dir);
			}
			#print "$quota , $Analysis_dir , $flag_is_too_full\n";
			if ($flag_is_too_full == 1 and $flag_hold_output == 0 ) {
				print OUT "[Job Hold]\n";
				$flag_hold_output = 1 ; 
			}
			if ($flag_is_too_full == 0  and $flag_hold_output == 1 ) {
				print OUT "[Job Release]\n";
				$flag_hold_output = 0 ; 
			}
			my $run_num = run_count(\%Alljob,\%Runjob, \%stat_p , $flag_is_too_full); ## store job information in stat_p ; 
			my $tmp_finish_num = finishJob(\%all_cycle_job , \%finish_blocks , $job_cmd , $max_cycle); ## include finish and break job
			#print "$tmp_finish_num\n";
			if ($tmp_finish_num == -1 ){
				sleep $Interval;
			}else{
				if ($finish_num < $tmp_finish_num){  ### if there is new finish job , output information
					$finish_num = $tmp_finish_num;
					print OUT "[Process]: $finish_num/$total_job_number finished\n";
					print OUT "[Finished]:\t" . join("\t" , sort(keys(%finish_blocks))) . " \n";
				}
			}
			
			#print "$flag_is_too_full $i $Maxjob $run_num \n";
			#die  if ($flag_is_too_full != 1);
			$Maxjob = modify_maxjob($Maxjob , $work_shell_file_error);
			if ($flag_is_too_full != 1 &&  ( $i < $Maxjob || ($run_num != -1 && $run_num < $Maxjob)) ) {
				my $jod_return = `$job_cmd $Shell[$i]`;
				#print "$jod_return";
				my $job_id = $1 if($jod_return =~ /Your job (\d+)/);
				$Alljob{$job_id} = $Shell[$i];  ## job id => shell file name
				$all_cycle_job{$job_id} = $Shell[$i];  ## job id => shell file name
				print STDERR "throw job $job_id in the $qsub_cycle cycle\n" if($Verbose);
				last;
			}else{
				#print "error waiting\n";
				print STDERR "wait for throwing next job in the $qsub_cycle cycle\n" if($Verbose);
				sleep $Interval;
			}
		}
	}
	chdir($current_dir); ##return into original directory 
	#print Dumper \%Alljob;
	###waiting for all jobs fininshed
	while (1) {
		$quota = readPreLogs($work_shell_file ,0 , $quota);
		if ( not defined $nodu){
			 $flag_is_too_full = is_too_full($quota , $Analysis_dir) ;
		}
		my $run_num = run_count(\%Alljob,\%Runjob,\%stat_p,$flag_is_too_full);
		my $tmp_finish_num = finishJob(\%all_cycle_job , \%finish_blocks, $job_cmd , $max_cycle , $Work_dir, $current_dir );
		#print Dumper \%stat_p;
		#print "$tmp_finish_num\t$finish_num/$total_job_number\n";
		#print Dumper \%finish_blocks;
		if ($tmp_finish_num == -1 ){
			sleep $Interval;
		}else{
			if ($finish_num < $tmp_finish_num){
				$finish_num = $tmp_finish_num;
				print OUT "[Process]: $finish_num/$total_job_number finished\n";
				#print  "[Process]: $finish_num/$total_job_number finished\n";
				print OUT "[Finished]:\t" . join("\t" , sort(keys(%finish_blocks))) . " \n";
			}
		}
		#print "$run_num\n";
		last if($run_num == 0);
		print STDERR "There left $run_num jobs runing in the $qsub_cycle cycle\n" if(defined $Verbose);
		#print Dumper \%stat_p;
		sleep $Interval;
	}

	print STDERR "All jobs finished, in the firt cycle in the $qsub_cycle cycle\n" if($Verbose);
	#print "all job finish\n";
	#print "sleep 20 seconds\n";
	sleep 20;
	#print "sleep 20 seconds\n";
	my $tmp_finish_num = finishJob(\%all_cycle_job , \%finish_blocks , $job_cmd , $max_cycle,  $Work_dir, $current_dir );
	#print "$tmp_finish_num\t$finish_num/$total_job_number\n";
	if ($tmp_finish_num == -1 ){
		sleep $Interval;
	}else{
		if ($finish_num < $tmp_finish_num){
			$finish_num = $tmp_finish_num;
			print OUT "[Process]: $finish_num/$total_job_number finished\n";
			print OUT "[Finished]:\t" . join("\t" , sort(keys(%finish_blocks))) . " \n";
		}
	}

	##run the secure mechanism to make sure all the jobs are really completed
	chdir($Work_dir); ##enter into the qsub working directoy
	#print Dumper \%Alljob;
	foreach my $job_id (sort keys %Alljob) {
		my $shell_file = $Alljob{$job_id};
		
		##read the .o file
		my $content;
		if (-f "$shell_file.o$job_id") {
			open IN,"$shell_file.o$job_id" || warn "fail $shell_file.o$job_id";
			$content = join("",<IN>);
			close IN;
		}
		##check whether the job has been killed during running time
		if ($content !~ /This-Work-is-Completed!$/) {
			$Error{$job_id} = $shell_file;
			print OUT "In qsub cycle $qsub_cycle, In $shell_file.o$job_id,  \"This-Work-is-Completed!\" is not found, so this work may be unfinished\n";
		}
		

	##make @shell for next cycle, which contains unfinished tasks
	@Shell = ();
	foreach my $job_id (sort keys %Error) {
		my $shell_file = $Error{$job_id};
		push @Shell,$shell_file;
	}
	
	$qsub_cycle++;
	if($qsub_cycle > $max_cycle){
		print OUT "\n\nProgram stopped because the reqsub cycle number has reached 10, the following jobs unfinished:\n";
		foreach my $job_id (sort keys %Error) {
			my $shell_file = $Error{$job_id};
			print OUT $shell_file."\n";
		}
		print OUT "Please check carefully for what errors happen, and redo the work, good luck!";
		die "\nProgram stopped because the reqsub cycle number has reached 10\n";
	}
	
	print OUT "All jobs finished!\n" unless(@Shell);

	chdir($current_dir); ##return into original directory 
	close OUT;
	print STDERR "The secure mechanism is performed in the $qsub_cycle cycle\n" if($Verbose);

	last unless(defined $Reqsub);
}

open OUT, ">>$work_shell_file_error" || die "fail create $$work_shell_file_error";
print OUT "name\tid\tVmem\tMax_mem\tnode\n";
foreach my$name(sort(keys %stat_p)){
	foreach my$id(sort(keys %{$stat_p{$name}})){
		my$vmem = mean_mem($stat_p{$name}{$id}{'vmem'})/1e9;
		my$max_men = max_mem($stat_p{$name}{$id}{'maxvmem'})/1e9;
		my$node = $stat_p{$name}{$id}{'node'};
		print OUT "$name\t$id\t$vmem\G\t$max_men\G\t$node\n";
	}
}

close OUT;
print STDERR "\nqsub-sge.pl finished\n" if($Verbose);

####################################################
################### Sub Routines ###################
####################################################

sub readPreLogs{
	my $prefix = shift;
	my $finish_blocks = shift;
	my $disk_quota = shift  ; 
	
	#print "ori : $disk_quota\n";
	#print " $prefix ; $disk_quota\n";
	$disk_quota ||= "10000000000000000000000G";
	#print "end : $disk_quota\n";

	my%discard ; 
	$finish_blocks ||= \%discard; 

	my @files = glob("$prefix.*.log");
	#print "$prefix\n";
	#print glob("$prefix.*.log");die;
	#print Dumper \@files;
	if (  length(@files) >  0 ){
		foreach my$a_file(@files){
			#print "$a_file\n";
			open PRELOG,$a_file||die;
			while(<PRELOG>){
				if (/^\[Finished\]/){
					my @tmp = split ;
					foreach my$i(@tmp[1..$#tmp]){
						$finish_blocks -> {$i} = 1 ;
					}
				}elsif(/^DISK_QUOTA/){
					#print;
					my @tmp = split;
					$disk_quota = $tmp[1];
				}
			}
			close PRELOG;
		}
	}else{
		print "$prefix.*.log is empty\n";
	}
	#print "aaaaaaaaaaaa\n";
	return $disk_quota; 
}
sub modify_maxjob{
	my $old = shift ; 
	my $logfile = shift;
	my $setting = 0 ; 
	open LOGFILE , $logfile || die "open $logfile finish\n";
	while(<LOGFILE>){
		if (/^Max_Jobs/){
			my @tmp = split;
			$setting = $tmp[1];
		}
	}
	close LOGFILE;
	return $setting; 

}
sub absolute_path{
	my($in_file,$out_file)=@_;
	my($current_path,$shell_absolute_path);

	#get the current path ;
	$current_path=`pwd`;   
	chomp $current_path;

	#get the absolute path of the input shell file;
	if ($in_file=~/([^\/]+)$/) {
		my $shell_local_path=$`;
		if ($in_file=~/^\//) {
			$shell_absolute_path = $shell_local_path;		
		}
		else{$shell_absolute_path="$current_path"."/"."$shell_local_path";}
	}	
	
	#change all the local path of programs in the input shell file;
	open (IN,"$in_file");
	open (OUT,">$out_file");
	while (<IN>) {
	    chomp;
		##s/>/> /; ##convert ">out.txt" to "> out.txt"
		##s/2>/2> /; ##convert "2>out.txt" to "2> out.txt"
	    my @words=split /\s+/, $_;
		
		##improve the command, add "./" automatically
		for (my $i=1; $i<@words; $i++) {
			if ($words[$i] !~ /\//) {
				if (-f $words[$i]) {
					$words[$i] = "./$words[$i]";
				}elsif($words[$i-1] eq ">" || $words[$i-1] eq "2>"){
					$words[$i] = "./$words[$i]";
				}
			}
			
		}
		for (my $i=0;$i<@words ;$i++) {
			if (($words[$i]!~/^\//) && ($words[$i]=~/\//)) {
				$words[$i]= "$shell_absolute_path"."$words[$i]";
				}
			}
	print OUT join("  ", @words), "\n";
	}
	close IN;
	close OUT;
}

sub max_mem{
	my$list = shift;
	my$max_m = 0;
	my$pp = '';
	foreach my$i(@$list){
		my$count = 0;
		if ($i=~m/(\d+?\.?\d+)([A-Z])/){
			$count = $1;
			$pp = $2;
			#my$count10 =$count*10;
			#print("before:$count\t$count10\t$pp\t");
			if ($pp eq 'G'){
				$count = $count * 1000000000;
			}elsif ($pp eq 'M'){
				$count = $count * 1000000;
			}elsif ($pp eq 'K'){
				$count = $count * 1000;
			}else{
				print("Memory is $pp\n");
			}
			#print("end:$count\t$pp\n");
		}
		$max_m = ($max_m < $count) ? $count : $max_m;
	}
	if ($max_m == 0){
		return 'NA';
	}else{
		return "$max_m";
	}
}


sub mean_mem{
	my$list = shift;
	my$n = 0 ; 
	my$total = 0;
	my$pp = '';
	foreach my$i(@$list){
		my$count = 0;
		if ($i=~m/(\d+?\.?\d+)([A-Z])/){
			$count = $1;
			$pp = $2;
			if ($pp eq 'G'){
				$count *= 1e9;
			}elsif ($pp eq 'M'){
				$count *= 1e6;
			}elsif ($pp eq 'K'){
				$count *= 1e3;
			}else{
				print("Memory is $pp\n");
			}
		}
		#print "$total\t$count\t$n\n";
		$total += $count;
		$n ++;
	}
	if ($n == 0 ){
		return 'NA';
	}else{
		return $total/$n;
	}
}

sub finishJob{
	my $all_p = shift;
	my $finish_job_id = shift;
	my $count_shell = shift;
	my $job_cmd =shift ;
	my $$max_cycle = shift;
	my $Work_dir = shift;
	my $current_dir = shift;
	
	#my %finish_job_id = %$finish_job_id;
	my %Alljob=%$all_p;
	my %ShellCount = %$count_shell; 
	my$finish_num = 0;

	my $user = `whoami`; chomp $user;
	my $qstat_result = `qstat -u $user`;
	chdir($Work_dir) if ($Work_dir);
	foreach my $job_id (sort keys %Alljob) {
		my $shell_file = $Alljob{$job_id};
		my $content  ;
		if (-f "$shell_file.o$job_id") {
			open IN,"$shell_file.o$job_id" || warn "fail $shell_file.o$job_id";
			$content = join("",<IN>);
			close IN;
		}else{
		}
		if ($content =~m/\[Job:(\d+)\] This-Work-is-Completed!$/) {
			$finish_num ++;
			#print "$1\n";
			$finish_job_id -> {$1}  = 1;
		}else{
			if ($ShellCount[$shell_file] <= $max_cycle){
				my $jod_return = `$job_cmd $shell_file`;
				my $new_job_id = $1 if($jod_return =~ /Your job (\d+)/);
				$ShellCount[$shell_file] += 1 ;
				print STDERR "reqsub $new_job_id for $job_id  in the $qsub_cycle cycle\n" if($Verbose);
			}else{

			}
		}
	}
	chdir($current_dir)  if($current_dir); 
	return $finish_num;
}

##get the IDs and count the number of running jobs
##the All job list and user id are used to make sure that the job id belongs to this program 
##add a function to detect jobs on the died computing nodes.
sub run_count {
	my $all_p = shift;
	my $run_p = shift;
	my $stat_p = shift;
	my $flag_disk = shift ; 
	my $run_num = 0;

	%$run_p = ();
	my $user = `whoami`; chomp $user;
	my $qstat_result = `qstat -u $user`;
	if ($qstat_result =~ /failed receiving gdi request/) {
		$run_num = -1;
		return $run_num; ##系统无反应
	}
	my @jobs = split /\n/,$qstat_result;
	#print Dumper $all_p; 
	foreach my $job_line (@jobs) {
		$job_line =~s/^\s+//;
		my @job_field = split /\s+/,$job_line;
		next if($job_field[3] ne $user);
		if (exists $all_p->{$job_field[0]}){
			
			my %died;
			died_nodes(\%died); ##the compute node is down, 有的时候节点已死，但仍然是正常状态
			#my $node_name = $1 if($job_field[7] =~ /(compute-\d+-\d+)/);
			my $node_name = $1 if($job_field[7] =~ /@(c\d+)\.local/);
			if ($flag_disk == 1 ){
				if ( !exists $died{$node_name} && ($job_field[4] eq "qw" || $job_field[4] eq "r" || $job_field[4] eq "t") ) {
					`qhold $job_field[0]`;
					print OUT "[Job Hold]\n";
					$run_num++;
					#print "hold $job_field[0]\n";
				}elsif ( !exists $died{$node_name} && ($job_field[4] eq "hqw" || $job_field[4] eq "hr" || $job_field[4] eq "ht") ) { 
					$run_num++;
				}else{
					`qdel $job_field[0]`;
				}
			}else{
				if ( !exists $died{$node_name} && ($job_field[4] eq "hqw" || $job_field[4] eq "hr" || $job_field[4] eq "ht") ) { 
					`qrls $job_field[0]`;
					print OUT "[Job Release]\n";
					#print "release $job_field[0]\n";
					$run_num++;
					#sleep 3;
				}elsif ( !exists $died{$node_name} && ($job_field[4] eq "qw" || $job_field[4] eq "r" || $job_field[4] eq "t") ) {  
					$run_p->{$job_field[0]} = $job_field[2]; ##job id => shell file name
					stat_info($stat_p , $job_field[0] , $job_field[2] );
					$stat_p->{$job_field[2]} -> {$job_field[0]} -> {'node'}  = $job_field[7];
					$run_num++;
				}else{
					`qdel $job_field[0]`;
					#print "qdel $job_field[0] $job_field[4] \n";
				}
			}
		}
	}

	return $run_num; ##qstat结果中的处于正常运行状态的任务，不包含那些在已死掉节点上的僵尸任务
}


##HOSTNAME                ARCH         NCPU  LOAD  MEMTOT  MEMUSE  SWAPTO  SWAPUS
##compute-0-24 lx26-amd64 8 - 15.6G - 996.2M -
sub died_nodes{
	my $died_p = shift;

	my @lines = split /\n/,`qhost`;
	shift @lines; shift @lines; shift @lines;  ##remove the first three title lines

	foreach  (@lines) {
		my @t = split /\s+/;
		my $node_name = $t[0];
		my $memory_use = $t[5];
		$died_p->{$node_name} = 1 if($t[3]=~/-/ || $t[4]=~/-/ || $t[5]=~/-/ || $t[6]=~/-/ || $t[7]=~/-/);
	}

}

sub stat_info{
	my $stat_p = shift;
	my $job_id = shift;
	my $job_name = shift;
	my($max_mem, $vmem) = parse_qstat($job_id);
	if ($max_mem == 0){
		return 0;
	}else{
		#print "$job_name\t$job_id\t$max_mem\t$vmem\n";
		#print Dumper $stat_p;
		if (not exists($stat_p->{$job_name})){
			%{$stat_p->{$job_name}->{$job_id}} = ('maxvmem'=>[$max_mem], 'vmem' => [$vmem]);
		}elsif (not exists($stat_p->{$job_name}->{$job_id})){
			%{$stat_p->{$job_name}->{$job_id}} = ('maxvmem'=>[$max_mem], 'vmem' => [$vmem]);
		}else{
			#print "$job_name\t$job_id\n";
			push @{$stat_p->{$job_name}->{$job_id}->{'vmem'}} , $vmem; 
			push @{$stat_p->{$job_name}->{$job_id}->{'maxvmem'}} , $max_mem; 
		}
	}
}

sub parse_qstat{
	my $job_id = shift;
	my $stat = `qstat -j $job_id`;
	my $maxvmen = $1 if ( $stat=~m/maxvmem=(\S+)/ ) ;
	my $vmem = $1 if ($stat =~m/vmem=(\S+?),/);
	if ($vmem eq 'N/A' or $vmem eq 'undef'){
		return 0;
	}else{
		return $maxvmen,$vmem;
	}
}

sub is_too_full{
	my$quota = shift;
	my$indir = shift;
	my$size = `du -s $indir`;
	my$qq ;
	if ($quota =~/^([0-9]+(.[0-9]+)?)G$/){
		$qq = $1 * 1e9;
	}elsif ($quota =~/^([0-9]+(.[0-9]+)?)M$/){
		$qq = $1 * 1e6;
	}elsif ($quota =~/^([0-9]+(.[0-9]+)?)K$/){
		$qq = $1 * 1e3;
	}elsif ($quota =~/^([0-9]+(.[0-9]+)?)$/){
		$qq = $quota;
	}
	if ($size > 0.95 * $qq){
		return 1 ;
	}else{
		return 0;
	}
}
