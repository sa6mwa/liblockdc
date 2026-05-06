if(NOT DEFINED OPENSSL_SOURCE_DIR)
  message(FATAL_ERROR "OPENSSL_SOURCE_DIR is required")
endif()

set(_mkbuildinf "${OPENSSL_SOURCE_DIR}/util/mkbuildinf.pl")
if(NOT EXISTS "${_mkbuildinf}")
  message(FATAL_ERROR "missing OpenSSL build-info generator: ${_mkbuildinf}")
endif()

file(READ "${_mkbuildinf}" _content)
set(_old [=[my $cflags = join(' ', @ARGV);
$cflags =~ s(\\)(\\\\)g;
$cflags = "compiler: $cflags";]=])
set(_new [=[my $cflags = "information not available";
$cflags = "compiler: $cflags";]=])

string(FIND "${_content}" "${_new}" _already_patched)
if(_already_patched LESS 0)
  string(FIND "${_content}" "${_old}" _patch_site)
  if(_patch_site LESS 0)
    message(FATAL_ERROR "OpenSSL build-info generator does not match expected content")
  endif()
  string(REPLACE "${_old}" "${_new}" _content "${_content}")
  file(WRITE "${_mkbuildinf}" "${_content}")
endif()

set(_mkinstallvars "${OPENSSL_SOURCE_DIR}/util/mkinstallvars.pl")
if(NOT EXISTS "${_mkinstallvars}")
  message(FATAL_ERROR "missing OpenSSL install-vars generator: ${_mkinstallvars}")
endif()

file(READ "${_mkinstallvars}" _installvars_content)
set(_installvars_original "${_installvars_content}")
string(REGEX REPLACE "\nprint STDERR \"DEBUG: all keys: \"[^\n]*\n" "\n" _installvars_content "${_installvars_content}")
string(REGEX REPLACE "\nprint STDERR \"DEBUG: LIBDIR = [^\n]*\n" "\n" _installvars_content "${_installvars_content}")
string(REGEX REPLACE "\nprint STDERR \"LIBDIR = [^\n]*\n" "\n" _installvars_content "${_installvars_content}")
string(REGEX REPLACE "\n    print STDERR \"DEBUG: \\$k = \\$v->\\[0\\] => \";\n" "\n" _installvars_content "${_installvars_content}")
string(REGEX REPLACE "\n    print STDERR \"\\$k = \\$v->\\[0\\]\\\\n\";\n" "\n" _installvars_content "${_installvars_content}")
set(_subdir_debug [=[        print STDERR "DEBUG: $k = ",
            (scalar @$v2 > 1 ? "[ " . join(", ", @$v2) . " ]" : $v2->[0]),
            " => ";
]=])
string(REPLACE "${_subdir_debug}" "" _installvars_content "${_installvars_content}")
set(_subdir_debug_done [=[        print STDERR join(", ",
                          map {
                              my $v = $values{$_};
                              "$_ = " . (scalar @$v > 1
                                         ? "[ " . join(", ", @$v) . " ]"
                                         : $v->[0]);
                          } ($k, $kr)),
            "\n";
]=])
string(REPLACE "${_subdir_debug_done}" "" _installvars_content "${_installvars_content}")

if(NOT _installvars_content STREQUAL _installvars_original)
  file(WRITE "${_mkinstallvars}" "${_installvars_content}")
endif()
