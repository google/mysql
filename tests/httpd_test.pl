#!/usr/bin/perl -w

# Small program to perform a HTTP GET over tcp of unix sockets.

use IO::Socket::UNIX;
use IO::Socket::INET;

if ($#ARGV == -1) {
  print "Usage:\n";
  print "  $0 0.0.0.0:8080\n";
  print "  $0 0.0.0.0:8080 health\n";
  print "  $0 0.0.0.0:8080 var\n";
  print "  $0 /tmp/mysql_httpd.sock var\n";
  exit;
}

if (-e $ARGV[0]) {
  $sock = IO::Socket::UNIX->new(
    Peer => $ARGV[0],
    Type => SOCK_STREAM)
    or die $@;
} else {
  @host_port = split(/:/, $ARGV[0]);
  $sock = IO::Socket::INET->new(
    PeerHost => $host_port[0],
    PeerPort => $host_port[1],
    Type => SOCK_STREAM,
    Proto => "tcp")
    or die $@;
}

print $sock "GET /$ARGV[1]\r\n\r\n";

$body = 0;
while (<$sock>) {
  if ($body) {
    print $_;
  } elsif ($_ =~ /^\r$/) {
    $body = 1;
  }
}

close($sock);
