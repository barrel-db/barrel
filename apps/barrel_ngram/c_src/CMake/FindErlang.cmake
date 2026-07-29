# Find Erlang
# This module finds if Erlang is installed and determines where the
# include files and libraries are. This code sets the following variables:
#
#  ERLANG_RUNTIME    = the full path to the Erlang runtime
#  ERLANG_COMPILE    = the full path to the Erlang compiler
#  ERLANG_ERTS_PATH    = the full path to the Erlang erts path
#  ERLANG_ERTS_INCLUDE_PATH = /include appended to ERLANG_ERTS_PATH

SET(ERLANG_BIN_PATH
    $ENV{ERLANG_HOME}/bin
    /opt/bin
    /sw/bin
    /usr/bin
    /usr/local/bin
    /opt/local/bin
    /opt/homebrew/bin
   )

FIND_PROGRAM(ERLANG_RUNTIME
  NAMES erl
  PATHS ${ERLANG_BIN_PATH}
  )

FIND_PROGRAM(ERLANG_COMPILE
  NAMES erlc
  PATHS ${ERLANG_BIN_PATH}
  )

EXECUTE_PROCESS(COMMAND
  erl -noshell -eval "io:format(\"~s\", [code:root_dir()])" -s erlang halt
  OUTPUT_VARIABLE ERLANG_OTP_ROOT_DIR)

EXECUTE_PROCESS(COMMAND
  erl -noshell -eval "io:format(\"~s\",[filename:basename(code:lib_dir('erts'))])" -s erlang halt
  OUTPUT_VARIABLE ERLANG_ERTS_DIR)

MESSAGE(STATUS "Using OTP root: ${ERLANG_OTP_ROOT_DIR}")
MESSAGE(STATUS "Using erts version: ${ERLANG_ERTS_DIR}")

SET(ERLANG_ERTS_PATH ${ERLANG_OTP_ROOT_DIR}/${ERLANG_ERTS_DIR})
SET(ERLANG_ERTS_INCLUDE_PATH ${ERLANG_OTP_ROOT_DIR}/${ERLANG_ERTS_DIR}/include)

MARK_AS_ADVANCED(
  ERLANG_RUNTIME
  ERLANG_COMPILE
  ERLANG_ERTS_PATH
  ERLANG_ERTS_INCLUDE_PATH
  )
