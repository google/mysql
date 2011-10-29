// Copyright 2008 Google Inc. All Rights Reserved.

#ifndef HTTPD_INCLUDED
#define HTTPD_INCLUDED
#include <pthread.h>

extern ulonglong http_thread_count;
extern uint httpd_port;

extern bool httpd;
extern uint httpd_port;
extern my_bool http_trust_clients;

/**
  Class definition for HTTPRequest.  This provides the implementation
  methods for the various GET requests for server status.
*/
class HTTPRequest
{
private:
  /**
    Allocate a two dimensional array of size rows*columns.  Memory
    is taken from the memory-pool.
    Return an array, or NULL if there were memory allocation problems.
  */
  void **AllocArray(int rows, int columns);

  /**
    Functions for appending string data to the HTML response header
    and body being created.
    These functions return true on error and false on success.
  */
  bool WriteBodyFmt(const char *fmt, ...);
  bool WriteBodyFmtVaList(const char *fmt, va_list ap);

  bool WriteBody(const char *buff)
  {
    return resp_body_.append(buff);
  }
  bool WriteBody(const char *buff, int bufLen)
  {
    return resp_body_.append(buff, bufLen);
  }
  bool WriteHeader(const char *buff)
  {
    return resp_header_.append(buff);
  }
  bool WriteHeader(const char *buff, int bufLen)
  {
    return resp_header_.append(buff, bufLen);
  }

  /**
    Functions for writing bits of HTML table markup.
  */
  bool WriteTableHeader(const char *title, const char *const *headings);
  bool WriteTableRowStart(void);
  bool WriteTableRowEnd(void);
  bool WriteTableEnd();
  bool WriteTableColumn(long long value);
  bool WriteTableColumn(long value);
  bool WriteTableColumn(unsigned long long value);
  bool WriteTableColumn(unsigned long value);
  bool WriteTableColumn(const char *value);
  bool WriteTableColumn(const char *host, int port);
  bool WriteTableColumn(double value);

  /**
    These are /var helper functions to generate MySQL status information.
    They all return 0 on success or errno upon errors.
  */
  int var_GenVars(const char *prefix, SHOW_VAR *vars);
  int var_ShowStatus();
  int var_ShowInnoDBStatus();
  int var_ShowVariables();
  int var_ProcessList();
  int var_UserStats();
  int var_TableStats();
  int var_IndexStats();
  int var_MasterStatus();
  int var_PrintVar(const char *fmt, ...);

  /**
    These are the /status helper functions for generating HTML tables
  */
  int status_ProcessListing(time_t current_time);
  int status_MasterStatus();
  int status_SlaveStatus();
  int status_UserStats();
  int status_TableStats();
  int status_IndexStats();

  void insertVar(uchar *base, uchar *end);

  THD *thd_;
  NET *net;
  String resp_body_, resp_header_;
  MEM_ROOT req_mem_root;

  /*
    List of variables requested via the /var?var=var1:var2:... url.
  */
  LIST *var_head;

public:

  HTTPRequest(THD *thd)
    : thd_(thd), resp_body_(NULL), resp_header_(NULL)
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

  ~HTTPRequest()
  {
    if (var_head)
      list_free(var_head, 1);

    free_root(&req_mem_root, 0);
  }

  const char *ResponseBody() { return resp_body_.ptr(); }
  const char *ResponseHeader() { return resp_header_.ptr(); }
  int ResponseBodyLength() { return resp_body_.length(); }
  int ResponseHeaderLength() { return resp_header_.length(); }

  /**
    Generate a HTTP response header
  */
  bool GenerateHeader(int code, bool html);

  /**
    Generate a HTTP error message.
  */
  bool GenerateError(const char *msg);

  bool parseURLParams();

  /**
    Functions that implement the GET requests.
    All functions return 0 on success and errno upon error.
  */
  int var();
  int health();
  int status();
  int portmapz();
  int quitquitquit();
  int abortabortabort();
};
#endif
