Datasets V3
-----------

### Background

Kart's internal repository structure - currently called "Datasets V3" - has been through a few iterations:

Kart Version | Kart's name at the time | Repository structure version
--- | --- | ---
0.0 to 0.1 | Snowdrop | Datasets V0
0.2 to 0.4 | Sno | Datasets V1
0.5 to 0.8 | Sno | Datasets V2
0.9 | Kart | Datasets V2
0.10 | Kart | Datasets V3 (but v2 still supported)

Datasets V3 is a storage formats for database tables where each table row is stored in a separate file. This means table rows can be stored using git-style version control, resulting in a storage format for a database-table which has version history.

The main improvement of Datasets V3 is how the rows are divided into different folders (or "trees" in git terminology) for more efficient storage when there are a large number of revisions and features. See [DATASETS_v2](DATASETS_v2.md) for more information on the preivous system.

The main improvement of Datasets V2 is that the schema of a table can be changed in isolation without having to rewrite every row in the table. Rows that were written with a previous schema are adapted to fit the current schema when read. See [DATASETS_v1](DATASETS_v1.md) for more information on the previous system.

To upgrade a Kart repository to the latest supported repository structure, run
`kart upgrade SOURCE DEST` where `SOURCE` is the path to the existing repo, and `DEST` is the path to where the upgraded repo will be created. This will rewrite your repository history — all commit information is preserved but the commit identifiers will all change. Merging changes across upgrades will not work out.

The following is a technical description of Datasets V3.

### Overall structure

A V3 dataset is a folder named `.table-dataset` that contains two folders. These are `meta` which contains information about the database table itself - its title, description, and schema - and 'feature' which contains the table rows. The schema file contains the structure of the table rows, ie the names and type of each column. The "name" of the V2 Dataset is the path to the `.table-dataset` folder.

For example, here is the basic folder structure of a dataset named contours/500m:

```
contours/
contours/500m/
contours/500m/.table-dataset/
contours/500m/.table-dataset/meta/
contours/500m/.table-dataset/meta/title              # Title of the dataset
contours/500m/.table-dataset/meta/description        # Description of the dataset
contours/500m/.table-dataset/meta/schema.json        # Schema of the dataset
contours/500m/.table-dataset/meta/...                # Other dataset metadata

contours/500m/.table-dataset/feature/...             # Database table rows
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

The following data types are supported by Kart, generally these follow the SQL standard data type categories. When a versioned Kart dataset is converted to a database table (ie, when `kart checkout` updates the working copy) then these Kart data types will be converted to equivalent data types in the database table, depending on what is supported by the database in question.

* `boolean`
  - stores `true` or `false`.
* `blob`
  - stores a string of bytes.
* `date`
  - stores year + month + day. The timezone that should be used to interpret this (if any) is not stored.
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
  - stores a 24 hour time as hour + minute + second. The timezone that should be used to interpret this (if any) is not stored.
* `timestamp`
  - stores a date + time. The timezone that should be used to interpret this is not stored, with one exception: the entire column can be defined as being in UTC in the column schema.

##### Extra type info

Certain types have extra attributes that help specify how the type should be stored in a database. They don't affect how Kart stores the data - and they don't necessarily affect all database types - for instance, setting a maximum length of 10 characters in a column with `"dataType": "text"` won't be enforced in a SQLite since it doesn't enforce maximum lengths.

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

###### Extra type info for `"dataType": "timestamp"`
- `timezone`
  * Eg: `"timezone": "UTC"`
  * The timezone that should be used to interpret the timestamp. The only valid values are `"UTC"` and `null`. If the timezone is `null`, that means that the timestamp's timezone (if any) is not stored in Kart, and therefore interpreting the timestamps correctly must be performed by a client with the appropriate context (ie, perhaps the client knows all stored timestamps are in local time at the client's location).


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

Features are stored at a path based on their primary key, so that an update to the feature that doesn't change its primary key will cause it to be overwritten in place. More information is provided below under [Feature paths](#feature-paths).

### Messagepack encoding

[MessagePack](https://msgpack.org/) can serialise everything that JSON can serialise, plus byte strings. For MessagePack to be able to serialise features containing any of the Kart-supported data types, sometimes the values to be serialised are converted to a more generic type first. The following serialisation logic is used:

* `boolean` - serialised as a boolean.
* `blob` - serialised as a byte string.
* `date` - serialised as a string, with the format `YYYY-MM-DD`
* `float` - serialised as a float.
* `geometry` - See Geometry encoding section below.
* `integer` - serialised as an integer.
* `interval` - serialised as a string, in [ISO8601 Duration](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, ie `PnYnMnDTnHnMnS`.
* `numeric` - serialised as a string, in decimal format eg `123` for a whole number or `123.456` if there is a fractional part.
* `text` - serialised as a string.
* `time` - serialised as a string, with the format `hh:mm:ss.ssss` and without a timezone. The fractions of a second may be omitted.
* `timestamp` - serialised as a string, in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format with `T` as the separator and without a timezone, ie `YYYY-MM-DDThh:mm:ss.ssss`. The fractions of a second may be omitted.

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

**Note on axis-ordering:** As required by the GeoPackageBinary format, which Kart uses internally for geometry storage, Kart's axis-ordering is always *(longitude/easting/x, latitude/northing/y, z, m)*. Following the GeoJSON specification, this same axis-ordering is also used in Kart's JSON and GeoJSON output.

### Feature paths

Every feature is stored at a path based on its primary key, so that an update to the feature that doesn't change its primary key will cause it to be overwritten in place. The primary key value can be transformed into its path and back into a primary key value without losing any information - for this reason, the values for primary key columns are not included in the contents of a feature file, since they can be inferred from the file's name.

A feature path might look like this:

`A/A/A/B/kU0=`

There are two parts to this: the path to the file - `A/A/A/B` - and the filename itself - `kU0=`.

#### Feature path filename

The filename is the more important part, and it is generated in the following manner:

`urlsafe_b64encode(msgpack.packb(primary_key_value_array))`

In the example feature path above, there is only one primary key column, and the feature being stored is the feature with primary key 77. So the primary key values are an array of length one containing 77: `[77]`. So the filename was generated as follows:

`[77]` -> MessagePack -> `bytes([0x91, 0x4d])` -> Base64 -> `kU0=`

#### Path to the feature file

For technical reasons, it is best if only a relatively small number of features are stored together in a single directory, and similarly if only a small number of directories are stored together in a single directory. Ideally, the features created at the same time or likely to be edited at the same time should be stored together, rather than spread out among all the other features - so, neighbouring primary key values should be neighbouring file paths where possible.

The exact system used to generate the path to the file depends on a few parameters which are stored in the dataset as an extra meta item called `path-structure.json`. The path structure might look like this:

```json
{
  "scheme": "int",
  "branches": 64,
  "levels": 4,
  "encoding": "base64"
}
```

The `"scheme": "int"` tells us that this path-structure is used for a dataset which has a single primary key column of type integer, and that value will be used directly to generate the path to the file. (The only other supported scheme is `"msgpack/hash"` - see below).

The next two parameters - `"branches": 64, "levels": 4` indicate that there are 4 levels of directory hierarchy, and at each level, there are up to 64 different directories branching out, such that a dataset with a huge number of features will have them spread across `64 ** 4 = 16777216` leaf-node directories - so a dataset could have `64 ** 5 = 1073741824` features and no directory would contain more than 64 directories or features. (Directories are only created when needed, so a dataset with only one feature with primary key 1 would create only four nested folders in which to store it, eg `A/A/A/A`.)

Each directory is named after a character in the [URL-safe Base64 alphabet](https://en.wikipedia.org/wiki/Base64#The_URL_applications) - this is the `"encoding": "base64"`, and this encoding only supports a branch factor of 64. The other valid encoding is `"hex"`, which supports a branch factor of 16 or 256.

So to encode the example before where the primary-key-value-array is `[77]` - since the scheme is "int" we know there is only one primary key value, an integer, which we can use as input for the subsequent steps: `77`. Encoding an integer (rather than a string of bytes) using Base64 works similarly to encoding integers in other bases such as hexadecimal. A quick primer: 0 is `A`, 1 is `B`, 64 is `BA`, and 77 is `BN`. We pad the left side with `A` (which stands for `0`) as needed: `AAABN`, and we remove the last character since we want to only change the path every 64 features, not every feature, giving us `AAAB`. (Feature filenames already have their own scheme which distiguishes them from every other feature in the same folder). Treating this as a path 4 levels long gives us `A/A/A/B`.

So, feature with primary key values `[77]` would be written at `A/A/A/B/kU0=` using this path-structure.

###### Example with a very large primary key:

`[1234567890]` -> Base64 -> `BJlgLS` -> remove last character, take next 4 last characters as path -> `J/l/g/L`

The filename would be encoded as before:

`[1234567890]` -> MessagePack -> `bytes([0x91, 0xce, 0x49, 0x96, 0x02, 0xd2])` -> Base64 -> `kc5JlgLS`

Giving a complete feature path of: `J/l/g/L/kc5JlgLS`

##### Alternate scheme - msgpack/hash

This scheme doesn't keep similarly named features near each other, so the "int" scheme is preferred when available. However, this scheme is more generic and works with any number of primary key columns, of any type.

The method for turning a primary key into a path to a file is now as follows:

`encode(sha256(msgpack.packb(primary_key_value_array)))`

So if we started with `[77]` again, we would turn it into a string of bytes as follows:

`[77]` -> MessagePack -> `bytes([0x91, 0x4d])` -> SHA256 -> `bytes([0x3c, 0x57, 0x8e, 0x75, ...])`

For the encoding step, as many bits as are needed are taken from the start of this bytestring and encoded to Base64 or hex in order to make the path. Assuming we use the same parameters as last time, four levels of base64 requires `4 * 6 = 24` bits, so this would work like so:

`bytes([0x3c, 0x57, 0x8e, 0x75, ...])` -> Base64 encode first 24 bits -> `PFeO` -> treat as path -> `P/F/e/O`

So, feature with primary key values `[77]` would be written at `P/F/e/O/kU0=` using this path-structure.

The paths to the files are more opaque in this scheme and provide less information about the feature's primary keys - however, just as in the last scheme, the feature's filename by itself can be decoded back into the primary key values. The paths are simply there to spread out the features for performance reasons.

#### Legacy path-structure

Datasets V2 only supports a single path structure, which is not stored in the dataset, but hard-coded. If no path-structure information is stored in the dataset, then the Datasets V2 structure is assumed. The Datasets V2 structure uses the following path-structure parameters (though these are implied, not stored in the repository):

```json
{
  "scheme": "msgpack/hash",
  "branches": 256,
  "levels": 2,
  "encoding": "hex"
}
```

See [DATASETS_v2](DATASETS_v2.md).

### Valid Dataset Names

Datasets have names, which can actually be hierarchical paths, e.g. `hydro/soundings`. Kart enforces the following rules about these paths:

* Paths may contain most unicode characters
* Paths must not contain any ASCII control characters (codepoints 00 to 1F), or any of the characters `:`, `<`, `>`, `"`, `|`, `?`, or `*`
* Paths must begin with a letter or an underscore (`_`).
* No path component may end with a `.` or a ` ` (space)
* Path components may not be any of these [reserved Windows filenames](https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file?redirectedfrom=MSDN#naming-conventions): `CON`, `PRN`, `AUX`, `NUL`, `COM1`, `COM2`, `COM3`, `COM4`, `COM5`, `COM6`, `COM7`, `COM8`, `COM9`, `LPT1`, `LPT2`, `LPT3`, `LPT4`, `LPT5`, `LPT6`, `LPT7`, `LPT8`, `LPT9`.
* Repositories may not contain more than one dataset with names that differ only by case.

Additionally, backslashes (`\`) in dataset paths are converted to forward slashes (`/`) when imported.

These rules exist to help ensure that Kart repositories can be checked out on a range of operating systems and filesystems.
