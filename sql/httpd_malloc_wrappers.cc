// Copyright 2008 Google Inc. All Rights Reserved.

/*
  Implementation of wrappers around non-standard MallocExtension (can be taken
  from gperftools).
*/

// This must be included first to play nice with min and max macros.
#include <string>
#include <malloc_extension.h>
#include <my_global.h>
#include <m_string.h>

#include "httpd_malloc_wrappers.h"


void GetMallocHeapGrowthStacks(std::string* stacks)
{
#ifdef GOOGLE_TCMALLOC
  MallocExtension::instance()->GetHeapGrowthStacks(stacks);
#else
  *stacks = "This feature requires GOOGLE_TCMALLOC.";
#endif
}

void GetMallocHeapSample(std::string* sample)
{
#ifdef GOOGLE_TCMALLOC
  MallocExtension::instance()->GetHeapSample(sample);
#else
  *sample = "This feature requires GOOGLE_TCMALLOC.";
#endif
}

void GetMallocStats(std::string* stats)
{
#ifdef GOOGLE_TCMALLOC
  char buf[2048];
  unsigned int len = sizeof(buf);

  bzero(buf, len);
  MallocExtension::instance()->GetStats(buf, len-1);

  *stats = buf;
#else
  *stats = "This feature requires GOOGLE_TCMALLOC.";
#endif
}

void ReleaseMallocFreeMemory(std::string* result)
{
#ifdef GOOGLE_TCMALLOC
  MallocExtension::instance()->ReleaseFreeMemory();
  *result = "OK";
#else
  *result = "This feature requires GOOGLE_TCMALLOC.";
#endif
}
