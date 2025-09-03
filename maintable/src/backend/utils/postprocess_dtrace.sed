#-------------------------------------------------------------------------
# sed script to postprocess dtrace output
#
# Copyright (c) 2008-2025, maintableQL Global Development Group
#
# src/backend/utils/postprocess_dtrace.sed
#-------------------------------------------------------------------------

# We editorialize on dtrace's output to the extent of changing the macro
# names (from MAINTABLEQL_foo to TRACE_MAINTABLEQL_foo) and changing any
# "char *" arguments to "const char *".

s/MAINTABLEQL_/TRACE_MAINTABLEQL_/g
s/( *char \*/(const char */g
s/, *char \*/, const char */g
