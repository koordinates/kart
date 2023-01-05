# Python Dependencies

Kart uses [pip-tools](https://pip-tools.readthedocs.io/en/latest/) to manage
Python dependencies for releases, test, and development.



### To update the requirements

1. do a CMake+VCPKG build
2. update the `requirements/*.in` files with the dependency changes
2. `cmake --build build --target py-requirements`
3. run `cmake --build build` to install the new dependencies
4. check & commit the changes to `requirements/*.in` and `requirements/*.txt`

### To upgrade the requirements

1. do a CMake+VCPKG build
2. `cmake --build build --target py-requirements-upgrade`
3. run `cmake --build build` to install the new dependencies
4. check & commit the changes to `requirements/*.txt`
