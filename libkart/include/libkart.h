/*
 * libkart C ABI.
 *
 * Conventions:
 *   - Fallible functions return int rc: 0 = ok, -1 = error. On error, kart_last_error()
 *     returns a message (valid until the next libkart call on the same thread).
 *   - Handles are uint64_t; 0 is never valid. Free repos/datasets with kart_*_free.
 *   - Returned buffers (byte pointer + length out-params) are malloc'd by libkart; release them
 *     with kart_free(). An absent/None value yields rc 0 with *out == NULL and *out_len == 0.
 *   - Returned text buffers are NOT NUL-terminated; use the accompanying length.
 */
#ifndef LIBKART_H
#define LIBKART_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* repo */
int kart_repo_open(const char *path, uint64_t *out_repo);
void kart_repo_free(uint64_t repo);
int kart_repo_table_dataset_version(uint64_t repo, int *out_version);
int kart_repo_list_datasets(uint64_t repo, const char *refish, uint8_t **out_json,
                            size_t *out_len);
/* commits_json: JSON array of 40-hex commit oids. query_json: {"pk": [..]} or
 * {"filter": {col: val}}. out: JSON [{"commit": hex, "path": "...", "oid": hex}, ...]
 * (path is repo-root-relative). */
int kart_repo_find_feature_blobs(uint64_t repo, const char *commits_json,
                                 const char *dataset_path, const char *query_json,
                                 uint8_t **out, size_t *out_len);
/* substitutions_json: {"<40-hex old blob oid>": "<base64 new content>"}.
 * backup_ref_prefix: nullable; non-NULL prefixes must start with "refs/".
 * out: JSON {"commits_rewritten": n, "blobs_written": n, "refs_updated": [...],
 * "backup_refs": [...]} */
int kart_repo_rewrite_history(uint64_t repo, const char *substitutions_json,
                              const char *backup_ref_prefix, uint8_t **out, size_t *out_len);
/* out: raw blob content for a 40-hex oid (caller reads e.g. generated-pks.json). */
int kart_repo_blob(uint64_t repo, const char *oid_hex, uint8_t **out, size_t *out_len);

/* dataset */
int kart_dataset_open(uint64_t repo, const char *refish, const char *path, uint64_t *out_ds);
void kart_dataset_free(uint64_t ds);
int kart_dataset_type(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_schema_json(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_crs_wkt(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_meta_item(uint64_t ds, const char *name, uint8_t **out, size_t *out_len);
/* pk_values_json: JSON array e.g. "[3]" or "[\"abc\"]". out: path string (relative to
 * the dataset dir). */
int kart_dataset_feature_path(uint64_t ds, const char *pk_values_json, uint8_t **out,
                              size_t *out_len);

/* feature / tile (caller supplies the raw git blob bytes) */
int kart_feature_geometry(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out,
                          size_t *out_len);
/* updates_json: JSON object {column_name: new_value}. out: new blob bytes. */
int kart_feature_update_blob(uint64_t ds, const uint8_t *blob, size_t blob_len,
                             const char *updates_json, uint8_t **out, size_t *out_len);
int kart_tile_summary_json(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out,
                           size_t *out_len);

/* gpkg geometry */
int kart_gpkg_is_empty(const uint8_t *g, size_t n, int *out);
int kart_gpkg_geometry_type(const uint8_t *g, size_t n, int *out);
int kart_gpkg_envelope(const uint8_t *g, size_t n, int only_2d, int calc, double *out6,
                       int *out_count);
int kart_gpkg_to_wkb(const uint8_t *g, size_t n, uint8_t **out, size_t *out_len);

/* misc */
const char *kart_last_error(void);
void kart_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* LIBKART_H */
