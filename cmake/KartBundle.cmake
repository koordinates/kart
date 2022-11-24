include(PyCreateVirtualEnvironment)

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/bundleEnv.requirements.txt
     "pyinstaller~=5.6.2\n" "macholib>=1.8;sys_platform=='darwin'\n")

createvirtualenvironment(bundleEnv REQUIREMENTS_TXT "bundleEnv.requirements.txt")

set(PYINSTALLER_ENV "BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}" "PYTHONPATH=${bundleEnv_PURELIB_DIR}")

add_custom_command(
  OUTPUT pyinstaller.stamp
  DEPENDS bundleEnv kart.spec # cli
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/
  COMMAND
    ${CMAKE_COMMAND} -E env ${PYINSTALLER_ENV} -- ${VENV_PY} -m PyInstaller --clean -y --workpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/ --distpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/ kart.spec
  VERBATIM
  COMMENT "Running PyInstaller")

# # fix up dylibs which should be symlinks (cd $(SRC)/platforms/macos/dist/Kart.app/Contents/MacOS/
# \ && for library in `find . -name \*.dylib  | xargs basename`; do \ if [ -L
# $(SRC)/venv/lib/$$library ]; then \ ln -sf `readlink $(SRC)/venv/lib/$$library` $$library; \ fi; \
# done)

add_custom_target(
  bundle
  DEPENDS pyinstaller.stamp
  COMMENT "Bundle Kart for installation & packaging")
