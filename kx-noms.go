package main

import (
    "bytes"
    "database/sql"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/attic-labs/noms/go/datas"
    "github.com/attic-labs/noms/go/spec"
    "github.com/attic-labs/noms/go/types"
    "github.com/google/uuid"
    "github.com/urfave/cli"
    _ "github.com/mattn/go-sqlite3"
)




func nomsValueFromDbValue(vrw types.ValueReadWriter, value interface{}) types.Value {
    switch value := value.(type) {
    case string:
        return types.String(value)
    case bool:
        return types.Bool(value)
    case float64:
    case int64:
        return types.Number(value)
    case nil:
        return nil
    case time.Time:  // timestamp -> ISO string
        return types.String(value.Format(time.RFC3339))
    case []uint8:  // geom/Blob
        return types.NewBlob(vrw, bytes.NewReader(value))
    }
    return types.String(fmt.Sprintf("%v", value))
}

func nomsValueFromDbRow(vrw types.ValueReadWriter, row map[string]interface{}) (string, types.Struct) {
    fields := make(types.StructData, len(row))
    for k, v := range row {
        nv := nomsValueFromDbValue(vrw, v)
        if nv != nil {
            k := types.EscapeStructField(k)
            fields[k] = nv
        }
    }

    featureKey := uuid.New().String()
    return featureKey, types.NewStruct("Feature", fields)
}

func loadGeopackage(gpkg_path string, gpkg_table string, noms_dataset_path string) error {
    log.Printf("%s/%s → %s", gpkg_path, gpkg_table, noms_dataset_path)

    noms_ds, err := spec.ForDataset(noms_dataset_path)
    if err != nil {
        log.Fatalf("Could not access noms dataset: %s\n", err)
    }
    noms_db := noms_ds.GetDatabase()
    defer noms_ds.Close()

    gpkg_db, err := sql.Open("sqlite3", gpkg_path)
    if err != nil {
        log.Fatalf("Could not access geopackage database: %s\n", err)
    }
    defer gpkg_db.Close()
    
    var row_count int
    err = gpkg_db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %q;", gpkg_table)).Scan(&row_count)
    if err != nil {
        log.Fatalf("Checking geopackage table (%s): %s", gpkg_table, err)
    }
    log.Printf("%s: %d rows", gpkg_table, row_count)
    if row_count < 0 {
        log.Fatal("No rows found")
    }

    rows, err := gpkg_db.Query(fmt.Sprintf("SELECT * FROM %q;", gpkg_table))
    if err != nil {
        log.Fatalf("Querying geopackage table (%s): %s", gpkg_table, err)
    }

    cols, err := rows.Columns()
    if err != nil {
        panic(err)
    }

    colvals := make([]interface{}, len(cols))
    rowMapEditor := types.NewMap(noms_db).Edit()
    for rows.Next() {
        colassoc := make(map[string]interface{}, len(cols))
        for i, _ := range colvals {
            colvals[i] = new(interface{})
        }
        if err := rows.Scan(colvals...); err != nil {
            panic(err)
        }
        for i, col := range cols {
            colassoc[col] = *colvals[i].(*interface{})
            //log.Printf("%s[%T]: %v", col, colassoc[col], colassoc[col])
        }
        
        key, data := nomsValueFromDbRow(noms_db, colassoc)
        rowMapEditor.Set(types.String(key), data)
    }
    rows.Close()

    rowMap := rowMapEditor.Map()
    log.Printf("Parsed %d rows", rowMap.Len())
    noms_db.Commit(noms_ds.GetDataset(), rowMap, datas.CommitOptions{})

    return nil
}

func updateGeopackage(noms_dataset_path string, gpkg_path string, gpkg_table string) error {
    log.Printf("%s → %s/%s", noms_dataset_path, gpkg_path, gpkg_table)

    noms_ds, err := spec.ForDataset(noms_dataset_path)
    if err != nil {
        log.Fatalf("Could not access noms dataset: %s\n", err)
    }
    noms_db := noms_ds.GetDatabase()
    defer noms_ds.Close()

    gpkg_db, err := sql.Open("sqlite3", gpkg_path)
    if err != nil {
        log.Fatalf("Could not access geopackage database: %s\n", err)
    }
    defer gpkg_db.Close()

    result, err := gpkg_db.Exec(fmt.Sprintf("DELETE FROM %q;", gpkg_table))
    if err != nil {
        log.Fatalf("Truncating geopackage table (%s): %s", gpkg_table, err)
    }

    if headValue, ok := noms_ds.GetDataset().MaybeHeadValue(); !ok {
        log.Printf("HEAD is empty")
        return nil
    } else {
        // type assertion to convert HEAD to Map
        featureMap := headValue.(types.Map)
        if featureMap.Empty() {
            log.Printf("No Features found")
            return nil
        }
        
        featureMap.IterAll(func(k string, v types.Struct) {
            log.Printf("Feature k=%v v=%v", k, v)
        })
    }
    return nil
}

func main() {
    app := cli.NewApp()

    app.Commands = []cli.Command{
        {
            Name: "load-gpkg",
            Usage: "Load a GeoPackage into a noms database",
            ArgsUsage: "GEOPACKAGE TABLE NOMS_DATASET",
            Action: func(c *cli.Context) error {
                return loadGeopackage(c.Args()[0], c.Args()[1], c.Args()[2])
            },
            Before: func(c *cli.Context) error {
                if len(c.Args()) != 3 {
                    return cli.NewExitError("Invalid arguments", 2)
                }
                return nil
            },
        },
        {
            Name: "update-gpkg",
            Usage: "Update a GeoPackage from a noms dataset",
            ArgsUsage: "NOMS_DATASET GEOPACKAGE TABLE",
            Action: func(c *cli.Context) error {
                return updateGeopackage(c.Args()[0], c.Args()[1], c.Args()[2])
            },
            Before: func(c *cli.Context) error {
                if len(c.Args()) != 3 {
                    return cli.NewExitError("Invalid arguments", 2)
                }
                return nil
            },
        },
    }

    err := app.Run(os.Args)
    if err != nil {
        log.Fatal(err)
    }
}
