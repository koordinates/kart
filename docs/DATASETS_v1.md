# Datasets V1 (& earlier)

Repository Layout:

## v0.2 with working copy

    myproject/
      .git/                          # git stuff lives under here
        config                       # includes pointer to working-copy

      readme.txt                     # random file managed by git

      path/to/mydataset/             # dataset name is 'dataset' here
        legend.jpg                   # random file managed by git
        .sno-table/                  # features for mydataset live under here in git. Excluded in git WC
          sno.idxi                   # spatial index (optional)
          sno.idxd                   # spatial index (optional)
          meta/
            version
            gpkg_contents
            gpkg_metadata            # metadata content (optional)
            gpkg_metadata_reference  # metadata <> table pointers (optional)
            sqlite_table_info
            primary_key
            gpkg_geometry_columns
            gpkg_spatial_ref_sys
          1a/                        # sha1 of feature pk, first 2 hex chars
            2b/                      # sha1 of feature pk, second 2 hex chars
              bAsE64=                # feature (msgpack-encoded). Name is base64-encoded msgpack of PK value

      otherdataset/                  # dataset name is 'dataset' here
        .sno-table/                  # features for mydataset live under here in git. Excluded in git WC
        documentation.pdf            # random file managed by git

      myproject.gpkg                 # working copy for project. Ignored in git WC


## v0.0

    nz_primary_parcels_taranaki/
      meta/
        gpkg_contents
        gpkg_geometry_columns
        gpkg_metadata
        gpkg_metadata_reference
        gpkg_spatial_ref_sys
        sqlite_table_info
        version
      features/
        fffd/
          fffdf8fa-e14d-4ac9-a9ba-0bb9f263a69e/
            affected_surveys
            appellation
            calc_area
            fid
            geom
            id
            land_district
            parcel_intent
            statutory_actions
            survey_area
            titles
            topology_type

# GPKG working copies (v0, v1 is similar)

### How the `__kxg_map` (IDMAP) table works

`state` field:

* 0 = unchanged
* -1 = deleted
* 1 = edited


TABLE<br>fid|<br>att|IDMAP<br>uuid|<br>fid|<br>state
-------|---------------|-------|-------|-----
1      |a              |123    |1      |0
2      |b              |124    |2      |0
3      |c              |125    |3      |0
6      |e              |126    |6      |0
7      |e              |127    |7      |0


```sql
UPDATE t SET att=cc WHERE fid=3
INSERT INTO t (fid, att) VALUES (4, 'd')
DELETE FROM t WHERE fid=1
```


TABLE<br>fid|<br>att|IDMAP<br>uuid|<br>fid|<br>state
-------|---------------|-------|-------|-----
.      |.              |123    |1      |-1
2      |b              |124    |2      |0
3      |cc             |125    |3      |1
4      |d              |NULL   |4      |0
6      |e              |126    |6      |0
7      |e              |127    |7      |0


```sql
UPDATE t SET fid=5 WHERE fid=2
```


TABLE<br>fid|<br>att|IDMAP<br>uuid|<br>fid|<br>state
-------|---------------|-------|-------|-----
.      |.              |123    |1      |-1
5      |b              |124    |5      |1
3      |cc             |125    |3      |1
4      |d              |NULL   |4      |0
6      |e              |126    |6      |0
7      |e              |127    |7      |0


```sql
UPDATE t SET att=bb WHERE fid=5
UPDATE t SET att=ccc WHERE fid=3
UPDATE t SET att=dd WHERE fid=4
```


TABLE<br>fid|<br>att|IDMAP<br>uuid|<br>fid|<br>state
-------|---------------|-------|-------|-----
.      |.              |123    |1      |-1
5      |bb             |124    |5      |1
3      |ccc            |125    |3      |1
4      |dd             |NULL   |4      |1
6      |e              |126    |6      |0
7      |e              |127    |7      |0


```sql
UPDATE t SET fid=2 WHERE fid=5
UPDATE t SET fid=1 WHERE fid=4
UPDATE t SET fid=9 WHERE fid=7
```


TABLE<br>fid|<br>att|IDMAP<br>uuid|<br>fid|<br>state
-------|---------------|-------|-------|-----
.      |.              |123    |1      |-1
2      |bb             |124    |2      |1
3      |ccc            |125    |3      |1
1      |dd             |NULL   |1      |1
6      |e              |126    |6      |0
9      |e              |127    |9      |1


* feature 123 was deleted
* feature 124 was edited  (att=b -> bb)
* feature 125 was edited  (att=c -> ccc)
* feature fid=1 was added (fid= -> 1; att= -> dd)
* feature 127 was edited  (fid=7 -> 9)
