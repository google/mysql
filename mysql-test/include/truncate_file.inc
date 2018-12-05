# ==== Purpose ====
#
# Truncate a given file to a certain size.
#
# ==== Usage ====
#
# --let $file= path
# [--let $size= number]
# --source include/truncate_file.inc
#
# Parameters:
#
# $file
#   What file to truncate.
#
# $size
#   The size of the resulting file. If no size is specified a length of zero
#   is assumed.

if (!$file)
{
  --echo Please assign a file name to $file!!
  exit;
}

if (`SELECT 'x$size' = 'x'`)
{
  --let $size= 0
}

let TRUNCATE_FILE= $file;
let TRUNCATE_SIZE= $size;

perl;
use Env;
Env::import('TRUNCATE_FILE', 'TRUNCATE_SIZE');
open FILE, '+<', $TRUNCATE_FILE || die "Can not open file $file";
truncate FILE, $TRUNCATE_SIZE;
close FILE;
EOF
