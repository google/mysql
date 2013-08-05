// Copyright 2008 Google Inc. All Rights Reserved.

#ifndef HTTPD_INCLUDED
#define HTTPD_INCLUDED
#include <pthread.h>

extern ulonglong httpd_thread_count;
extern ulong httpd_bind_addr;
extern char *httpd_bind_addr_str;
extern uint httpd_port;

extern bool httpd;
extern uint httpd_port;
extern my_bool httpd_trust_clients;
extern char *httpd_unix_port;

/**
  Class definition for Http.  This provides the implementation
  methods for the various GET requests for server status.
*/

class Http_request
{
private:
  /**
    Allocate a two dimensional array of size rows*columns.  Memory
    is taken from the memory-pool.
    Return an array, or NULL if there were memory allocation problems.
  */
  void **alloc_array(int rows, int columns);

  /**
    Functions for appending string data to the HTML response header
    and body being created.
    These functions return true on error and false on success.
  */
  bool write_body_fmt(const char *fmt, ...);
  bool write_body_fmt_va_list(const char *fmt, va_list ap);

  bool write_body_htmlencode(const char *buff, int buffLen);
  bool write_body_htmlencode(const char *buff)
  {
    return write_body_htmlencode(buff, strlen(buff));
  }

  bool write_body(const char *buff)
  {
    return resp_body_.append(buff);
  }

  bool write_body(const char *buff, int bufLen)
  {
    return resp_body_.append(buff, bufLen);
  }

  bool write_header(const char *buff)
  {
    return resp_header_.append(buff);
  }

  bool write_header(const char *buff, int bufLen)
  {
    return resp_header_.append(buff, bufLen);
  }

  /**
    Functions for writing bits of HTML table markup.
  */
  bool write_table_header(const char *title, const char *const *headings);
  bool write_table_row_start(void);
  bool write_table_row_end(void);
  bool write_table_end();
  bool write_table_column(long long value);
  bool write_table_column(long value);
  bool write_table_column(unsigned long long value);
  bool write_table_column(unsigned long value);
  bool write_table_column(const char *value);
  bool write_table_column(const char *host, int port);
  bool write_table_column(double value);

  /**
    These are /var helper functions to generate MySQL status information.
    They all return 0 on success or errno upon errors.
  */
  int var_gen_vars(const char *prefix, SHOW_VAR *vars);
  int var_show_status();
  int var_show_innodb_status();
  int var_show_variables();
  int var_process_list();
  int var_user_statistics();
  int var_table_statistics();
  int var_index_statistics();
  int var_master_status();
  int var_print_var(const char *fmt, ...);
  bool var_need_print_var(const char *var);

  /**
    These are the /status helper functions for generating HTML tables.
  */
  int status_process_list(time_t current_time);
  int status_master_status();
  int status_slave_status();
  int status_user_statistics();
  int status_table_statistics();
  int status_index_statistics();

  void insert_var(uchar *base, uchar *end);

  THD *thd_;
  NET *net;
  String resp_body_, resp_header_;
  MEM_ROOT req_mem_root;

  /*
    List of variables requested via the /var?var=var1:var2:... url.
  */
  LIST *var_head;

public:
  Http_request(THD *thd)
    : thd_(thd), resp_body_(), resp_header_()
  {
    net= &thd_->net;

    /*
      32Kbytes here should be enough to cover the memory required
      to buffer a /var or /status page, without needing extra memory
      allocations.  However, if more space is required then it will be
      allocated in 32K chunks.
    */
    init_alloc_root(&req_mem_root, 32768, 32768);

    var_head= NULL;
  }

  ~Http_request()
  {
    if (var_head)
      list_free(var_head, 1);

    free_root(&req_mem_root, 0);
  }

  const char *response_body() { return resp_body_.ptr(); }
  const char *response_header() { return resp_header_.ptr(); }
  int response_body_length() { return resp_body_.length(); }
  int response_header_length() { return resp_header_.length(); }

  /**
    Generate a HTTP response header
  */
  bool generate_header(int code, bool html);

  /**
    Generate a HTTP error message.
  */
  bool generate_error(const char *msg);

  bool parse_url_params();

  /**
    Functions that implement the GET requests.
    All functions return 0 on success and errno upon error.
  */
  int var();
  void health();
  void growth();
  void heap();
  void malloc();
  void release_memory();
  void status();
  void quitquitquit();
  void abortabortabort();
};
#endif
