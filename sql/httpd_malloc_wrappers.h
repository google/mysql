// Copyright 2008 Google Inc. All Rights Reserved.

#ifndef HTTPD_MALLOC_WRAPPERS_INCLUDED
#define HTTPD_MALLOC_WRAPPERS_INCLUDED


void GetMallocHeapGrowthStacks(std::string* stacks);
void GetMallocHeapSample(std::string* sample);
void GetMallocStats(std::string* stats);
void ReleaseMallocFreeMemory(std::string* result);


#endif  /* HTTPD_MALLOC_WRAPPERS_INCLUDED */
