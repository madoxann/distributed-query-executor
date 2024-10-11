#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "ArrowTesting::arrow_testing_shared" for configuration "DEBUG"
set_property(TARGET ArrowTesting::arrow_testing_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(ArrowTesting::arrow_testing_shared PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_DEBUG "Boost::process"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarrow_testing.so.1800.0.0"
  IMPORTED_SONAME_DEBUG "libarrow_testing.so.1800"
  )

list(APPEND _cmake_import_check_targets ArrowTesting::arrow_testing_shared )
list(APPEND _cmake_import_check_files_for_ArrowTesting::arrow_testing_shared "${_IMPORT_PREFIX}/lib/libarrow_testing.so.1800.0.0" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
