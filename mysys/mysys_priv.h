/* Copyright (C) 2000 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <my_global.h>
#include <my_sys.h>

#ifdef USE_SYSTEM_WRAPPERS
#include "system_wrappers.h"
#endif

#ifdef HAVE_GETRUSAGE
#include <sys/resource.h>
#endif

#ifdef THREAD
#include <my_pthread.h>
extern pthread_mutex_t THR_LOCK_malloc, THR_LOCK_open, THR_LOCK_keycache;
extern pthread_mutex_t THR_LOCK_lock, THR_LOCK_isam, THR_LOCK_net;
extern pthread_mutex_t THR_LOCK_charset, THR_LOCK_time;
#else
#include <my_no_pthread.h>
#endif

/*
  EDQUOT is used only in 3 C files only in mysys/. If it does not exist on
  system, we set it to some value which can never happen.
*/
#ifndef EDQUOT
#define EDQUOT (-1)
#endif

void my_error_unregister_all(void);

/* checksum_io.c */
size_t chksum_read(File Filedes, uchar *Buffer, size_t Count, myf MyFlags);

size_t chksum_pread(File Filedes, uchar *Buffer, size_t Count,
                    my_off_t offset, myf MyFlags);

size_t chksum_pwrite(File Filedes, const uchar *Buffer, size_t Count,
                     my_off_t offset, myf MyFlags);

size_t chksum_write(File Filedes, const uchar *Buffer, size_t Count, myf MyFlags);

my_off_t chksum_tell(File fd, myf MyFlags);

my_off_t chksum_seek(File fd, my_off_t pos, int whence, myf MyFlags);
