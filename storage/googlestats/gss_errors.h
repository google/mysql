// Copyright 2013 Google Inc. All Rights Reserved.

#ifndef GSS_ERRORS_H
#define GSS_ERRORS_H

#include <my_base.h>

// This should be greater than HA_ERR_LAST (currently 163).
#define GSS_ERR_FIRST               300
// If the errors are updated, the corresponding messages in gss_err_msgs
// in ha_googlestats.cc should also be updated.
#define GSS_ERR_TIMEOUT             (GSS_ERR_FIRST + 0)
#define GSS_ERR_SOCKET_READ         (GSS_ERR_FIRST + 1)
#define GSS_ERR_COMPUTE_CHECKSUM    (GSS_ERR_FIRST + 2)
#define GSS_ERR_INDEX_INIT          (GSS_ERR_FIRST + 3)
#define GSS_ERR_MEMORY              (GSS_ERR_FIRST + 4)
#define GSS_ERR_SOCKET_WRITE        (GSS_ERR_FIRST + 5)
#define GSS_ERR_BAD_SIGNATURE       (GSS_ERR_FIRST + 6)
#define GSS_ERR_BAD_VERSION_NUM     (GSS_ERR_FIRST + 7)
#define GSS_ERR_NO_RESTART_KEY      (GSS_ERR_FIRST + 8)
#define GSS_ERR_BAD_HEADER_VALUES   (GSS_ERR_FIRST + 9)
#define GSS_ERR_DECOMPRESS_ERR      (GSS_ERR_FIRST + 10)
#define GSS_ERR_BAD_RESTART_KEY     (GSS_ERR_FIRST + 11)
#define GSS_ERR_GET_ROW             (GSS_ERR_FIRST + 12)
#define GSS_ERR_REQUEST_COLUMNS     (GSS_ERR_FIRST + 13)
#define GSS_ERR_BAD_SCAN_TYPE       (GSS_ERR_FIRST + 14)
#define GSS_ERR_BAD_INDEX_NAME      (GSS_ERR_FIRST + 15)
#define GSS_ERR_CONNECT_SERVER      (GSS_ERR_FIRST + 16)
#define GSS_ERR_SOCKET_CREATE       (GSS_ERR_FIRST + 17)
#define GSS_ERR_NO_SERVER           (GSS_ERR_FIRST + 18)
// Copy the last number.
#define GSS_ERR_LAST                (GSS_ERR_FIRST + 18)

#endif // GSS_ERRORS_H
