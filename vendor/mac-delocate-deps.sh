#!/bin/bash
set -eu

LIBREL="${2-}"

ls -lr "$1"
chmod -R a+r,u+rw "$1"/*

ln -s "." "$1/@loader_path"

echo "invoking 'delocate-path -L \"$LIBREL\" \"$1\"'"
delocate-path -L "$LIBREL" "$1"

rm "$1/@loader_path"
chmod -R a+r,u+rw "$1"/*

ls -lr "$1"

echo "updating library id values"
find "$1" -type f -name "*.dylib" -print0 | while read -d $'\0' P
do
    F=$(basename "$P")
    echo "invoking 'install_name_tool -id \"$F\" \"$P\"'"
    install_name_tool -id "$F" "$P"
done

ls -lr "$1"
