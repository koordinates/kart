#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <assert.h>
#include <math.h>
#include <time.h>

#include <sqlite3.h>

extern "C" {
    #include <list-objects-filter-extensions.h>
    #include "adapter_functions.h"
}

using std::string;
using std::vector;

namespace {

static const string INDEX_FILENAME = "feature_envelopes.db";

static const int OBJ_COMMIT = 1;
static const int OBJ_TREE = 2;
static const int OBJ_BLOB = 3;
static const int OBJ_TAG = 4;

class EnvelopeEncoder {
    // Encodes and decodes bounding boxes - (w, s, e, n) tuples in degrees longitude / latitude.

    // This is the number of bits-per-value used to store envelopes when writing to a fresh database.
    // When writing to an existing database, it will look to see how envelopes have been stored previously.
    // Increasing this parameter increases the accuracy of the envelopes, but each one takes more space.
    // This number must be even, so that four values take up a whole number of bytes.
    // This number can't exceed 32 bits without updating the code below to handle more than 128 bits of envelope.
    static const int DEFAULT_BITS_PER_VALUE = 20;

    const int BITS_PER_VALUE;
    const int BITS_PER_ENVELOPE;
    const int BYTES_PER_ENVELOPE;
    const uint32_t VALUE_MAX_INT;

    const int NUM_LO_BITS;
    const int NUM_LO_BYTES;
    const uint64_t MAX_LO_BITS;

    const int NUM_HI_BITS;
    const int NUM_HI_BYTES;
    const uint64_t MAX_HI_BITS;

    public:
    EnvelopeEncoder(int bits_per_value = 0):
        BITS_PER_VALUE(bits_per_value ? bits_per_value : DEFAULT_BITS_PER_VALUE),
        BITS_PER_ENVELOPE(BITS_PER_VALUE * 4),
        BYTES_PER_ENVELOPE(BITS_PER_ENVELOPE / 8),
        VALUE_MAX_INT((1 << BITS_PER_VALUE) - 1),

        NUM_LO_BITS(std::min(64, BITS_PER_ENVELOPE)),
        NUM_LO_BYTES(NUM_LO_BITS / 8),
        MAX_LO_BITS(NUM_LO_BITS == 64 ? std::numeric_limits<uint64_t>::max() : (1ull << NUM_LO_BITS) - 1),

        NUM_HI_BITS(std::max(0, BITS_PER_ENVELOPE - 64)),
        NUM_HI_BYTES(NUM_HI_BITS / 8),
        MAX_HI_BITS((1ull << NUM_HI_BITS) - 1) {}


    std::string encode(double w, double s, double e, double n) {
        // Encodes a (w, s, e, n) envelope where -180 <= w, e <= 180 and -90 <= s, n <= 90.
        // Scale each value to a unsigned integer of bitlength BITS_PER_VALUE such that 0 represents the min value (eg -180
        // for longitude) and 2**BITS_PER_VALUE - 1 represents the max value (eg 180 for longitude), then concatenates
        // the values together into a single unsigned integer of bitlength BITS_PER_VALUE, which is encoded to a byte array
        // of length BYTES_PER_ENVELOPE using a big-endian encoding.

        uint64_t hi_bits = 0, lo_bits = 0;
        lo_bits = encode_value(w, -180, 180, false);
        shift_left(&hi_bits, &lo_bits, BITS_PER_VALUE);
        lo_bits |= encode_value(s, -90, 90, false);
        shift_left(&hi_bits, &lo_bits, BITS_PER_VALUE);
        lo_bits |= encode_value(e, -180, 180, true);
        shift_left(&hi_bits, &lo_bits, BITS_PER_VALUE);
        lo_bits |= encode_value(n, -90, 90, true);

        assert (lo_bits <= MAX_LO_BITS);
        assert (hi_bits <= MAX_HI_BITS);

        char result[BYTES_PER_ENVELOPE];
        uintX_to_bytes_BE(hi_bits, NUM_HI_BITS, &result[0]);
        uintX_to_bytes_BE(lo_bits, NUM_LO_BITS, &result[NUM_HI_BYTES]);
        return std::string(result, BYTES_PER_ENVELOPE);
    }

    uint32_t encode_value(double value, double min_value, double max_value, bool round_up) {
        assert ((min_value <= value) && (value <= max_value));
        double normalised = (value - min_value) / (max_value - min_value);
        double scaled = normalised * VALUE_MAX_INT;
        uint32_t encoded = (uint32_t) (round_up ? ceil(scaled) : floor(scaled));
        assert (encoded <= VALUE_MAX_INT);
        return encoded;
    }

    void decode(const std::string& input, double* w, double* s, double* e, double* n) {
        // Inverse of encode.
        uint64_t hi_bits = 0, lo_bits = 0;
        bytes_to_uintX_BE(&hi_bits, NUM_HI_BITS, &input[0]);
        bytes_to_uintX_BE(&lo_bits, NUM_LO_BITS, &input[NUM_HI_BYTES]);

        assert (lo_bits <= MAX_LO_BITS);
        assert (hi_bits <= MAX_HI_BITS);

        *n = decode_value(lo_bits & VALUE_MAX_INT, -90, 90);
        shift_right(&hi_bits, &lo_bits, BITS_PER_VALUE);
        *e = decode_value(lo_bits & VALUE_MAX_INT, -180, 180);
        shift_right(&hi_bits, &lo_bits, BITS_PER_VALUE);
        *s = decode_value(lo_bits & VALUE_MAX_INT, -90, 90);
        shift_right(&hi_bits, &lo_bits, BITS_PER_VALUE);
        *w = decode_value(lo_bits & VALUE_MAX_INT, -180, 180);
    }

    double decode_value(uint32_t encoded, double min_value, double max_value) {
        assert (encoded <= VALUE_MAX_INT);
        double normalised = ((double) encoded) / VALUE_MAX_INT;
        return normalised * (max_value - min_value) + min_value;
    }

    void shift_left(uint64_t* hi_bits, uint64_t* lo_bits, int shift) {
        uint64_t carry_bits = (*lo_bits) >> (64 - shift);
        *hi_bits = (*hi_bits << shift) | carry_bits;
        *lo_bits = (*lo_bits << shift);
    }

    void shift_right(uint64_t* hi_bits, uint64_t* lo_bits, int shift) {
        uint64_t carry_bits = (*hi_bits) << (64 - shift);
        *hi_bits = (*hi_bits >> shift);
        *lo_bits = (*lo_bits >> shift) | carry_bits;
    }

    void uintX_to_bytes_BE(uint64_t input, int num_bits, char* output) {
        for (int i = 0; i < num_bits; i += 8) {
            *(output++) = (char) ((input >> (num_bits - i - 8)) & 0xff);
        }
    }

    void bytes_to_uintX_BE(uint64_t* output, int num_bits, const char* input) {
        const uint8_t* uinput = reinterpret_cast<const uint8_t*>(input);
        *output = 0;
        for (int i = 0; i < num_bits; i += 8) {
            *output |= ((uint64_t) *(uinput++)) << (num_bits - i - 8);
        }
    }
};

enum match_result {
    MR_MATCH,
    MR_NOT_MATCHED,
    MR_ERROR,
};

struct filter_context {
    int count = 0;
    int match_count = 0;
    uint64_t started_at = 0;
    sqlite3 *db = nullptr;
    sqlite3_stmt *lookup_stmt = nullptr;
    double w = 0, s = 0, e = 0, n = 0;
    EnvelopeEncoder *encoder = nullptr;
};

bool range_overlaps(double a1, double a2, double b1, double b2) {
    if (a1 > a2 || b1 > b2) {
        std::cerr << "Ranges don't make sense: " << a1 << " " << a2 << " " << b1 << " " << b2 << "\n";
        abort();
    }
    if (b1 < a1) {
        // `b` starts to the left of `a`, so they intersect if `b` finishes to the right of where `a` starts.
        return b2 > a1;
    }
    if (a1 < b1) {
        // `a` starts to the left of `b`, so they intersect if `a` finishes to the right of where `b` starts.
        return a2 > b1;
    }
    // They both have the same left edge, so they must intersect unless one of them is zero-width.
    return b2 != b1 && a2 != a1;
}

bool cyclic_range_overlaps(double a1, double a2, double b1, double b2) {
    // Changes [170, -170] to [170, 190] (and so on) - makes sure a1 <= a2 and b1 <= b2.
    if (a1 > a2) {
        a2 += 360;
    }
    if (b1 > b2) {
        b2 += 360;
    }
    if (range_overlaps(a1, a2, b1, b2)) {
        return true;
    }
    // The ranges don't obviously overlap, but they might if we increase the smaller one by 360.
    // For example, if they are [-170, -160] and [160, 210] - the smaller one is equivalent to [190, 200].
    if (a1 < b1) {
        a1 += 360;
        a2 += 360;
    } else {
        b1 += 360;
        b2 += 360;
    }
    return range_overlaps(a1, a2, b1, b2);
}

// Core function - decides whether a blob matches or not.

enum match_result sf_filter_blob(
    struct filter_context *ctx,
    const struct repository* repo,
    const struct object_id *oid,
    const string &path)
{
    // We are only spatial-filtering features - all non-feature data matches automatically.
    if (path.find("/.sno-dataset/feature/") == string::npos
        && path.find("/.table-dataset/feature/") == string::npos) {
        return MR_MATCH;
    }

    sqlite3 *db = ctx->db;
    sqlite3_stmt *stmt = ctx->lookup_stmt;

    int sql_err = sqlite3_bind_blob(stmt, 1, sf_oid2hash(oid), sf_repo2hashsz(repo), SQLITE_TRANSIENT);
    if (sql_err) {
        std::cerr << "\nspatial-filter: Error: preparing lookup (" << sql_err << " @0): " << sqlite3_errmsg(db) << "\n";
        return MR_ERROR;
    }

    sql_err = sqlite3_step(stmt);
    if (sql_err == SQLITE_DONE) {
        sqlite3_reset(stmt);
        return MR_MATCH;
    }
    if (sql_err != SQLITE_ROW) {
        std::cerr << "\nspatial-filter: Error: querying (" << sql_err << "): " << sqlite3_errmsg(db) << "\n";
        sqlite3_reset(stmt);
        return MR_ERROR;
    }

    int num_bytes = sqlite3_column_bytes(stmt, 0);
    std::string envelope(static_cast<const char*>(sqlite3_column_blob(stmt, 0)), num_bytes);

    if (!ctx->encoder) {
        int bits_per_value = num_bytes * 8 / 4;
        ctx->encoder = new EnvelopeEncoder(bits_per_value);
    }

    double s, w, e, n;
    ctx->encoder->decode(envelope, &w, &s, &e, &n);

    bool overlaps = cyclic_range_overlaps(w, e, ctx->w, ctx->e) && range_overlaps(s, n, ctx->s, ctx->n);

    sqlite3_reset(stmt);

    return overlaps ? MR_MATCH : MR_NOT_MATCHED;
}

//
// Filter extension interface:
//

int sf_init(
    const struct repository *r,
    const char *filter_arg,
    void **context)
{
    std::vector<double> rect;
    std::stringstream ss_arg(filter_arg);
    double d;

    while (ss_arg >> d)
    {
        rect.push_back(d);
        if (ss_arg.peek() == ',')
            ss_arg.ignore();
    }
    if (rect.size() != 4) {
        std::cerr << "spatial-filter: Error: invalid bounds, expected '<lng_w>,<lat_s>,<lng_e>,<lat_n>'\n";
        return 2;
    }

    std::ostringstream ss_db(sf_repo2gitdir(r), std::ios_base::ate);
    ss_db << "/" << INDEX_FILENAME;

    sf_trace_printf("DB: %s\n", ss_db.str().c_str());

    struct filter_context *ctx = new filter_context();
    (*context) = ctx;
    ctx->w = rect[0];
    ctx->s = rect[1];
    ctx->e = rect[2];
    ctx->n = rect[3];

    if (sqlite3_open_v2(ss_db.str().c_str(), &ctx->db, SQLITE_OPEN_READONLY, NULL)) {
        std::cerr << "spatial-filter: Warning: not available for this repository - no objects will be omitted.\n";
        sqlite3_close(ctx->db);
        ctx->db = nullptr;
        return 0;
    }

    int sql_err;
    sqlite3_stmt *stmt;

    // prepare the lookup db query
    const string query_sql("SELECT envelope FROM feature_envelopes WHERE blob_id=?;");
    sql_err = sqlite3_prepare_v3(ctx->db,
                                 query_sql.c_str(),
                                 static_cast<int>(query_sql.size()+1),
                                 SQLITE_PREPARE_PERSISTENT,
                                 &ctx->lookup_stmt,
                                 NULL);
    if (sql_err) {
        std::cerr << "spatial-filter: Error: preparing lookup (" << sql_err << ") " << sqlite3_errmsg(ctx->db) << "\n";
        return 1;
    }

    sf_trace_printf("Query SQL: %s\n", sqlite3_expanded_sql(ctx->lookup_stmt));

    (*context) = ctx;
    return 0;
}

enum list_objects_filter_result sf_filter_object(
    const struct repository *repo,
    const enum list_objects_filter_situation filter_situation,
    struct object *obj,
    const char *pathname,
    const char *filename,
    enum list_objects_filter_omit *omit,
    void *context)
{
    struct filter_context *ctx = static_cast<struct filter_context*>(context);

    static const list_objects_filter_result LOFR_MARK_SEEN_AND_DO_SHOW =
        static_cast<list_objects_filter_result>(LOFR_MARK_SEEN | LOFR_DO_SHOW);

    if (ctx->count == 0) {
        ctx->started_at = getnanotime();
    }
    if (++ctx->count % 10000 == 0) {
        std::cerr << "Enumerating objects: " << ctx->match_count << "    (Spatial-filter has tested " << ctx->count << " objects)\r";
    }

    switch (filter_situation) {
        default:
            std::cerr << "spatial-filter: unknown filter_situation: " << filter_situation << "\n";
            abort();

        case LOFS_COMMIT:
            assert(sf_obj2type(obj) == OBJ_COMMIT);
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_TAG:
            assert(sf_obj2type(obj) == OBJ_TAG);
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_BEGIN_TREE:
            assert(sf_obj2type(obj) == OBJ_TREE);
            // Always include all tree objects.
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_END_TREE:
            assert(sf_obj2type(obj) == OBJ_TREE);
            return LOFR_ZERO;

        case LOFS_BLOB:
            assert(sf_obj2type(obj) == OBJ_BLOB);

            if (ctx->db == nullptr) {
                // We don't have a valid spatial index for this repository. Don't omit anything.
                return LOFR_MARK_SEEN_AND_DO_SHOW;
            }

            switch(sf_filter_blob(ctx, repo, sf_obj2oid(obj), pathname)) {
                case MR_ERROR:
                    abort();

                case MR_NOT_MATCHED:
                    *omit = LOFO_OMIT;
                    return LOFR_MARK_SEEN;

                case MR_MATCH:
                    ++ctx->match_count;
                    return LOFR_MARK_SEEN_AND_DO_SHOW;
            }
    }
}

void sf_free(const struct repository* r, void *context) {
    struct filter_context *ctx = static_cast<struct filter_context*>(context);

    double elapsed = (getnanotime() - ctx->started_at) / 1e9;
    std::cerr << "spatial-filter: " << ctx->count << "\n";
    sf_trace_printf(
        "count=%d matched=%d elapsed=%fs rate=%f/s average=%fus\n",
        ctx->count, ctx->match_count, elapsed, ctx->count/elapsed, elapsed/ctx->count*1e6
    );

    if (ctx->lookup_stmt != nullptr) {
        sqlite3_finalize(ctx->lookup_stmt);
    }
    if (ctx->db != nullptr) {
        sqlite3_close_v2(ctx->db);
    }
    if (ctx->encoder != nullptr) {
        delete ctx->encoder;
    }
    delete ctx;
}

}  // namespace

extern "C" {
extern const struct filter_extension filter_extension_spatial = {
    "spatial",
    &sf_init,
    &sf_filter_object,
    &sf_free,
};
}