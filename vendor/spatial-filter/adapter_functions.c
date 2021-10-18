#include <git-compat-util.h>
#include <hash.h>
#include <object.h>
#include <repository.h>
#include <trace.h>

// Allows access to C structs or functions that are defined
// in C++ incompatible headers.

void sf_trace_printf(const char* format, ...) {
    static struct trace_key key = TRACE_KEY_INIT(FILTER);
    struct strbuf buf = STRBUF_INIT;
    va_list args;
    va_start(args, format);
    strbuf_vaddf(&buf, format, args);
    va_end(args);
    trace_strbuf(&key, &buf);
    strbuf_release(&buf);
}

const struct object_id* sf_obj2oid(const struct object *obj) {
    return &obj->oid;
}

const unsigned char* sf_oid2hash(const struct object_id *oid) {
    return oid->hash;
}

const unsigned sf_obj2type(const struct object *obj) {
    return obj->type;
}

const char* sf_repo2gitdir(const struct repository *repo) {
    return repo->gitdir;
}

int sf_repo2hashsz(const struct repository *repo) {
    return repo->hash_algo->rawsz;
}
