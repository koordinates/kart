Approximated Types
------------------

Sno has a particular set of data types that it supports (see DATASETS_v2.md). The different types of working copies that Sno supports also have their own sets of data types that they support. Most Sno data types are well-supported by most working copies, but for those data types that are not, they have to be "approximated" by the closest type that is available in the working copy environment.

#### Example of how an approximated type works

For example, Sno's integer type can be configured to be 8, 16, 32 or 64 bits. The PostGIS working copy does not have an 8-bit integer type, so it is approximated as a 16 bit integer instead. This works as follows:

- A particular Sno dataset contains a column with the `8-bit integer` type.
- When checked out, the PostGIS working copy will contain an equivalent column, but with the `16-bit integer` type.
- All of the data stored in Sno for this column is written to the equivalent column in the PostGIS working copy. This is possible because every 8-bit integer can be stored in a 16-bit integer. This is a general principal of approximated types: the approximated type must be able to hold every possible value of the original data (but perhaps not in its original form).
- The user is able to modify the contents of this column to other 8-bit integer values and commit them.
- The user *may* modify the contents of this column to contain 16-bit integer values that would not fit in 8-bits of storage, but they will not be allowed to commit this change. Attempting to commit values that cannot be converted back to the original type fails with a message about a "schema violation".
- Any difference between the original column contents as stored within Sno, and the approximated column contents as stored in the working copy, is not considered to be a diff and cannot be committed if it is only due to the technical limitations of the working copy. In this example, the original 8-bit value for -1 is stored internally as `0xFF` and the "approximated" 16-bit value of -1 is stored as `0xFFFF` but these two values are considered equal and only differ due to the technical limitations of the working copy, so this difference will not show up when `sno diff` is run, and it cannot be committed. However, if the user then modifies the working copy to contain +1 instead of -1, then that difference is a decision made by the user, not a technical limitation, and so that will be visible when `sno diff` is run and can be committed.
- Any difference between the original column type a stored within Sno, and the approximated column type as stored in the working copy, is not considered to be a diff and cannot be commited as it is only due to a technical limitation in the working copy. In this example, the change from `8-bit integer` to `16-bit integer` will not be shown in the list of differences when `sno diff` is run. However, if the user were to modify the type of the column in the working copy from `16-bit integer` to `32-bit integer`, then that difference is a decision made by the user, not a technical limitation, and so that will be visible when `sno diff` is run and can be committed.

#### General principles for approximated types

Other type approximations differ in the details, but they all work similarly:
- An equivalent or broader type is substituted for the desired type in the working copy if the desired type doesn't exist in the working copy.
- Content or type diffs for that column are *not* shown and cannot be committed where they are only due to working copy limitations, not the user's decisions.
- Content or type diffs for that column *are* shown and can be committed where they are caused by the user making changes to the contents or column type
- Changing the contents of a column so that it cannot be converted back to the original type means that change cannot be committed; it results in a schema violation.

### Specific types and their approximations

#### boolean
- Supported by GPKG as `BOOLEAN`, PostGIS as `BOOLEAN`, SQL Server as `BIT`

#### blob
- Supported by: GPKG as `BLOB`, PostGIS as `BYTEA` SQL Server as `VARBINARY`

#### blob with maximum-length
- Supported by: GPKG as `BLOB(max-length)`, SQL Server as `VARBINARY(max-length)`
- Approximated in PostGIS as `BYTEA` (no maximum-length)
- It is a schema violation to try to commit a blob that is longer than the maximum length.

#### date
- Supported by: PostGIS as `DATE`, SQL Server as `DATE`
- Supported in GPKG as `DATE`, however, unlike dates in other working copies this type is a free-form text type, and so can contain arbitrary text.
- Date columns in GPKG will contain strings conforming to the [ISO 8601 date format](https://en.wikipedia.org/wiki/ISO_8601#Dates), ie `YYYY-MM-DD`.
- It is a schema violation to try to commit a `DATE` string in a GPKG working copy which doesn't conform to the [ISO 8601 date format](https://en.wikipedia.org/wiki/ISO_8601#Dates).

#### floating point (32-bit)
- Supported by: GPKG as `FLOAT`, PostGIS as `REAL`, SQL Server as `REAL`

#### floating point (64-bit)
- Supported by: GPKG as `REAL`, PostGIS as `DOUBLE PRECISION`, SQL Server as `FLOAT`

#### geometry
- Supported by: GPKG as `GEOMETRY`, PostGIS as `GEOMETRY`, SQL Server as `GEOMETRY`

#### integer (8-bit)
- Supported by: GPKG as `TINYINT`, SQL Server as `TINYINT`
- Approximated in PostGIS as `SMALLINT` (16-bit)
- It is a schema violation to try to commit an integer in a PostGIS working copy 8-bit integer column that will not fit in 8-bits (it must have a value beween -128 and 127 inclusive).

#### integer (16-bit)
- Supported by: GPKG as `SMALLINT`, PostGIS as `SMALLINT`, SQL Server as `SMALLINT`

#### integer (32-bit)
- Supported by: GPKG as `MEDIUMINT`, PostGIS as `INTEGER`, SQL Server as `INT`

#### integer (64-bit)
- Supported by: GPKG as `INTEGER`, PostGIS as `BIGINT`, SQL Server as `BIGINT`

#### interval
- Supported by: PostGIS as `INTERVAL`
- Approximated in GPKG as `TEXT`, SQL Server as `NVARCHAR` (ie, text)
- Interval columns in GPKG and SQL Server will contain strings conforming to the [ISO 8601 duration format](https://en.wikipedia.org/wiki/ISO_8601#Durations), ie `PxYxMxDTxHxMxS` (where each `x` is replaced with the number of years, months, days, hours, minutes or seconds respectively).
- It is a schema violation to try to commit a string in a GPKG or SQL Server working copy interval column that doesn't conform to the [ISO 8601 duration format](https://en.wikipedia.org/wiki/ISO_8601#Durations).

#### numeric
- Supported by: PostGIS as `NUMERIC`, SQL Server as `NUMERIC`
- Approximated in GPKG as `TEXT`
- Numeric columns in GPKG will contain decimal numbers as strings, ie `123.456`.
- It is a schema violation to try to commit a string in a GPKG working copy numeric column that isn't a decimal number.

#### text
- Supported by: GPKG as `TEXT`, PostGIS as `TEXT`, SQL Server as `NVARCHAR`

#### text with maximum length:
- Supported by: GPKG as `TEXT(max-length)`, PostGIS as `VARCHAR(max-length)`, SQL Server as `NVARCHAR(max-length)`

#### time
- Supported by: PostGIS as `TIME`, SQL Server as `TIME`
- Approximated in GPKG as `TEXT`
- Time columns in GPKG will contain strings conforming to the [ISO 8601 time format](https://en.wikipedia.org/wiki/ISO_8601#Times), without a timezone - ie, `HH:MM:SS.SSS`
- It is a schema violation to try to commit a string in a GPKG working copy time column that doesn't conform to the [ISO 8601 time format](https://en.wikipedia.org/wiki/ISO_8601#Times), without a timezone.

#### timestamp
- Supported by: PostGIS as `TIMESTAMPTZ`, SQL Server as `DATETIMEOFFSET`
- Supported in GPKG as `DATETIME`, however, unlike timestamps in other working copies this type is a free-form text type, and so can contain arbitrary text.
- Timestamp columns in GPKG will contain strings conforming to the [ISO 8601 datetime format](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations), and end with a `Z` indicating that they are in UTC time - ie `YYYY-MM-DDTHH:MM:SS.SSSZ`.
- It is a schema violation to try to commit a string in a GPKG working copy timestamp column that doesn't conform to the [ISO 8601 datetime format](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations) with timezone `Z`.

### GPKG is not type-safe

SQLite, which the GPKG spec is built upon, does not enforce that the contents of a column match its type. In a GPKG working copy, it is a schema violation to try to commit any contents into a column that doesn't match its type.
