Datasets V2
-----------

### Background

Sno 0.2 introduced Datasets V1, and Sno 0.5 introduced Datasets V2.
Datasets V1 and V2 are storage formats for database tables where each table row is stored in a separate file.
This means table rows can be stored using git-style version control, resulting in a storage format for a database-table which has version history.

Datasets V2 is very similar to Datasets V1 - the main difference is that the schema of a Datasets V2 table can be changed in isolation without having
to rewrite every row in the table. Rows that were written with a previous schema are adapted to fit the current schema when read.

In Sno 0.4, only Datasets V1 is supported. In Sno 0.5, both Datasets V1 and Datasets V2 are supported, but a particular Sno repository must be entirely one or the other. This can be selected by specifying either `sno init --repo-version=1` or `sno init --repo-version=2`. If this is not specified, the default is V1 from Sno 0.2 onwards, but the default is V2 starting at Sno 0.5.

The following is a technical description of Datasets V2.

### Overall structure

A V2 dataset is a folder named `.sno-dataset` that contains two folders. These are `meta` which contains information about the database table itself - its title, description, and schema - and 'feature' which contains the table rows. The schema file contains the structure of the table rows, ie the names and type of each column. The "name" of the V2 Dataset is the path to the `.sno-dataset` folder.

For example, here is the basic folder structure of a dataset named contours/500m:

```
contours/
contours/500m/
contours/500m/.sno-dataset/
contours/500m/.sno-dataset/meta/
contours/500m/.sno-dataset/meta/title              # Title of the dataset
contours/500m/.sno-dataset/meta/description        # Description of the dataset
contours/500m/.sno-dataset/meta/schema.json        # Schema of the dataset
contours/500m/.sno-dataset/meta/...                # Other dataset metadata

contours/500m/.sno-dataset/feature/...             # Database table rows
```

### Meta items

The following items are stored in the meta part of the dataset, and have the following structure.

#### `meta/title`
Contains the title of the dataset, encoded using UTF-8. The title is freeform text, clients could if they desired include HTML tags or markdown in the title.

#### `meta/description`
Contains the title of the dataset, encoded using UTF-8. The description is freeform text, clients could if they desired include HTML tags or markdown in the title.

#### `meta/schema.json`
Contains the current schema of the table, as a JSON array. Each item in the array represents a column in the table, and contains the name and type of that database column. A more in-depth explanation is found below under the heading "Syntax".

For example, here is the schema for a dataset containing building outlines:
```json
[
  {
    "id": "500f5ecc-c02c-e8db-052a-84efb4d08a00",
    "name": "fid",
    "dataType": "integer",
    "primaryKeyIndex": 0,
    "size": 64
  },
  {
    "id": "b8ae8ff3-1691-7f6f-bbe0-c03c648b6d67",
    "name": "geom",
    "dataType": "geometry",
    "geometryType": "MULTIPOLYGON",
    "geometryCRS": "EPSG:4326"
  },
  {
    "id": "4324e825-ffed-8aff-ceac-53a7dde7e2f7",
    "name": "building_id",
    "dataType": "integer",
    "size": 32
  },
  {
    "id": "878f8e4e-433a-b7bb-74d5-b360ccfb3607",
    "name": "name",
    "dataType": "text",
    "length": 250
  },
  {
    "id": "c8e75111-0506-a898-4d0e-ed1aa8c81280",
    "name": "last_modified",
    "dataType": "date",
  }
]
```

##### Syntax
Every JSON object in the array represents a column of the database table, and these objects are listed in the same order as the columns in the table. Each of these objects has at least the three required attributes - `id`, `name` and `dataType` - and some have a fourth optional attribute, `primaryKeyIndex`.

###### `id`
This is a unique ID used internally, the contents of the ID have no specific meaning. However, the ID of a column remains constant over its lifetime, even as its name or position in the array changes, so they can are used to recognise a column even if it has been renamed and moved.

###### `name`
This is the name of the column in the database table, as would be used in a SELECT statement. Column names must be unique within a dataset.

###### `dataType`
This is the type of data which is stored in this column. A complete list of allowed types is found in the "Data types" section below.

###### `primaryKeyIndex`
This controls whether or not this column is a primary key. If this value is `null` or not present, then the column is not a primary key. If it is any non-negative integer, then the column is a primary key. The first primary key column should have a `primaryKeyIndex` of `0`, the next primary key column should have `1`, and so on.

Those are all of the fields that apply to any column. Certain dataTypes can have extra fields that help specify the type of data that the column should hold - see the "Extra type info" section below.

##### Data types

The following data types are supported by sno. When a versioned sno dataset is converted to a database table (ie, when `sno checkout` updates the working copy) then these sno data types will be converted to equivalent data types in the database table, depending on what is supported by the database in question.

* `boolean`
  - stores `true` or `false`.
* `blob`
  - stores a string of bytes.
* `date`
  - stores year + month + day. The timezone that should be used interpret this is not stored.
* `float`
  - stores a floating point number using a fixed number of bits. Floating point values have reasonable but imperfect precision over a huge range.
* `geometry`
  - stores a well-known-text geometry eg a point or a polygon.
* `integer`
  - stores an integer value, using a fixed number of bits.
* `interval`
  - stores an interval of time as a number of years + months + days + hours + minutes + seconds
* `numeric`
  - stores a decimal number using a fixed number of digits of precision.
* `text`
  - stores a string of text, using the database's text encoding.
* `time`
  - stores hour + minute + second, and optionally the timezone that this time was recorded in.
* `timestamp`
  - stores a date + time, and optionally the timezone that this timestamp was recorded in.

##### Extra type info

Certain types have extra attributes that help specify how the type should be stored in a database. They don't affect how sno stores the data - and they don't necessarily affect all database types - for instance, setting a maximum length of 10 characters in a column with `"dataType": "text"` won't be enforced in a SQLite since it doesn't enforce maximum lengths.

If any of these attributes are not present, that has the same effect as if that attribute was present but was set to `null`.

The extra attributes that are supported are as follows:

###### Extra type info for `"dataType": "geometry"`
- `geometryType`
  * Eg `"geometryType": "MULTIPOLYGON ZM"`
  * A well-known-text (WKT) geometry type - eg "POINT", "LINESTRING", "MULTIPOLYGON", etc, optionally followed by a Z or M indicator if the data has a third dimension or a linear referencing system (or both).
- `geometryCRS`
  * Eg `"geometryCRS": "EPSG:2193"`
  * A string used to identify the Coordinate Reference System of the geometry. Often in the form `"EPSG:1234"` for a CRS in the EPSG registry, but for a custom CRS, any identifier could be chosen.
  * Can be `null` for an unspecified CRS.

###### Extra type info for `"dataType": "integer"`
- `size`
  * Eg `"size": 16`
  * The size of the integer in bits. Should be 8, 16, 32, or 64.

###### Extra type info for `"dataType": "float"`
- `size`
  * Eg `"size": 32`
  * The size of the floating point number in bits. Should be 32 or 64.

###### Extra type info for `"dataType": "text"`
- `length`
  * Eg: `"length": 100`
  * The maximum length of the text in characters.
  * Can be null if the maximum length is unbounded.

###### Extra type info for `"dataType": "numeric"`
- `precision`
  * The maximum number of total digits for the numeric type.
- `scale`
  * How many of the digits are to the right of the decimal point.
For example, the number "1234.5678" can be stored in a numeric type with a precision of 8 and a scale of 4.

#### `meta/legend/...`

The legend folder of the dataset contains data known as "legends" that are used when reading features. Features that are written using one schema could be read later once the schema has changed. A legend contains the minimal amount of information required to adapt the feature to the current schema. This information is just the list of column IDs from the schema at the time of writing. For example, if features were written using the schema in the section above, this would also result in the following legend being written:

```json
[
  "500f5ecc-c02c-e8db-052a-84efb4d08a00",
  "b8ae8ff3-1691-7f6f-bbe0-c03c648b6d67",
  "4324e825-ffed-8aff-ceac-53a7dde7e2f7",
  "878f8e4e-433a-b7bb-74d5-b360ccfb3607",
  "c8e75111-0506-a898-4d0e-ed1aa8c81280",
]
```

Another legend entry is added whenever an update to the schema is committed, and they are never modified or deleted. They are part of the internal structure of the dataset and they need not be viewed by the end user.

Each legend has a unique filename based on the sha256 hash of its contents. Legends are not stored using JSON, but in a binary encoding called [MessagePack](https://msgpack.org/) that has equivalent capabilities.

#### `meta/crs/{identifier}.wkt`
A dataset should contain coordinate-reference-system (CRS) definitions for any CRS needed to interpret its geometry. These are stored in [Well-Known-Text format](http://docs.opengeospatial.org/is/18-010r7/18-010r7.html) (WKT). The identifier that is part of the filename here should be the same as the `geometryCRS` identifier in the schema.

### Features

Every database table row is stored in its own file. It is stored as an array of values plus the name of the legend that should be used to read it. This array is serialised using [MessagePack](https://msgpack.org/), but for the sake of readability, the example below is shown in JSON.

For instance, a single feature might be stored as the following:
```json
[
  "204b9886d5dbd9fe3a7edb9a7a7dba699b5202f7",
  [
    1445288,
    "GP0001e61000000101cce1b0dce@7fx8f4Dc0",
    1260047,
    "Pukerua Bay Police Station",
    "2018-11-05"
  ]
]
```
Note that the first value is the name of the legend, and the remaining values are the values (but not keys) of the database row.

Decoding a feature works as follows - first, look up the legend with the given name from the `meta/legend/` directory. This will contain a list of column IDs. There will be the same number of column IDs as values, and stored in the same order, so that they can be combined together into key-value pairs:

```json
{
  "500f5ecc-c02c-e8db-052a-84efb4d08a00": 1445288,
  "b8ae8ff3-1691-7f6f-bbe0-c03c648b6d67": "GP0001e61000000101cce1b0dce@7fx8f4Dc0",
  "4324e825-ffed-8aff-ceac-53a7dde7e2f7": 1260047,
  "878f8e4e-433a-b7bb-74d5-b360ccfb3607": "Pukerua Bay Police Station",
  "c8e75111-0506-a898-4d0e-ed1aa8c81280": "2018-11-05"
}
```

Finally, the current schema is consulted to find out the current position and name of the columns with those IDs, so that a database row can be constructed. If a column is no longer part of the schema, the value for that column will be dropped from the feature. If a new column has been added to the schema since this feature was written, the feature will have a `NULL` value for that column. The end result will be a feature that conforms to the current database schema - something like the following:

```json
{
  "fid": 1445288,
  "geom": "GP0001e61000000101cce1b0dce@7fx8f4Dc0",
  "building_id": 1260047,
  "name": "Pukerua Bay Police Station",
  "star_rating": null,
  "last_modified": "2018-11-05"
}
```

Features are stored at a filename that contains a Base64 encoding of their primary key, so that an update to the feature that doesn't change its primary key will cause it to be overwritten in place.

### Messagepack encoding

[MessagePack](https://msgpack.org/) can serialise everything that JSON can serialise, plus byte strings. For MessagePack to be able to serialise features containing any of the sno-supported data types, sometimes the values to be serialised are converted to a more generic type first. The following serialisation logic is used:

* `boolean` - serialised as a boolean.
* `blob` - serialised as a byte string.
* `date` - serialised as a string, with the format `YYYY-MM-DD`
* `float` - serialised as a float.
* `geometry` - See Geometry encoding section below.
* `integer` - serialised as an integer.
* `interval` - serialised as a string, in [ISO8601 Duration](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, ie `PnYnMnDTnHnMnS`.
* `numeric` - serialised as a string, in decimal format eg `123` for a whole number or `123.456` if there is a fractional part.
* `text` - serialised as a string.
* `time` - serialised as a string, with the format `hh:mm:ss.ssss`, optionally with the suffix `Z` indicating that the time is in UTC. The fractions of a second may be omitted.
* `timestamp` - serialised as a string, in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format with `T` as the separator, ie `YYYY-MM-DDThh:mm:ss.ssss`, optionally with the suffix `Z` indicating that the time is in UTC. The fractions of a second may be omitted.

In those cases where a certain part of the representation may be omitted - in practise, that part will be omitted if it is zero. If it is non-zero it will always be included.

#### Geometry encoding

Geometries are converted to byte strings before they are serialised using MessagePack. The geometry bytestring is marked as being a MessagePack extension with the extension code `"G"` (71). The encoding used to serialise the geometry is as follows.

Geometries are encoded using the Standard GeoPackageBinary format specified in [GeoPackage v1.3.0 §2.1.3 Geometry Encoding](http://www.geopackage.org/spec/#gpb_data_blob_format), with additional restrictions:

1. Geometries must use the StandardGeoPackageBinary type.
2. GeoPackage binary headers must always use little-endian byte ordering.
3. The WKB geometry must always use little-endian byte ordering.
4. All non-empty geometries must have an envelope, except for POINT types:
   - Points and empty geometries have no envelope.
   - Geometries with a Z component have an XYZ envelope.
   - Other geometries have an XY envelope.
5. The `srs_id` is always 0, since this information not stored in the geometry object but is stored on a per-column basis in `meta/schema.json` in the `geometryCRS` field.
