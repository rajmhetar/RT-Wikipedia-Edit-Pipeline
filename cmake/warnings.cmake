# Apply strict warnings to a single target without touching global flags.
# Usage: target_enable_warnings(my_target)
function(target_enable_warnings target)
  target_compile_options(${target} PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:
      /W4
      /wd4068   # unknown pragma (Clang/GCC pragmas appear in some headers)
    >
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:
      -Wall
      -Wextra
      -Wpedantic
      -Wconversion
      -Wshadow
    >
  )
endfunction()
