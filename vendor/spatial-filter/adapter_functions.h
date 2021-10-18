#ifndef SPATIAL_FILTER_ADAPTER_FUNCTIONS_H
#define SPATIAL_FILTER_ADAPTER_FUNCTIONS_H

// C++ compatible header that allows access to C structs
// or functions that are defined in C++ incompatible headers.

// From trace.h
uint64_t getnanotime(void);

// Delegates to trace_strbuf from trace.h
void sf_trace_printf(const char* format, ...);

struct object;
struct object_id;

// Accessors for struct object from object.h
const struct object_id* sf_obj2oid(const struct object *obj);
const unsigned sf_obj2type(const struct object *obj);

// Accessors for struct object_id from hash.h
const unsigned char* sf_oid2hash(const struct object_id *oid);

struct repository;

// Accessors for struct repository from repository.h
const char* sf_repo2gitdir(const struct repository *repo);
int sf_repo2hashsz(const struct repository *repo);

#endif /* SPATIAL_FILTER_ADAPTER_FUNCTIONS_H */
