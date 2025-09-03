/*-------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the maintableQL version string
 *
 * Copyright (c) 1998-2025, maintableQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 *-------------------------------------------------------------------------
 */

#include "maintable.h"

#include "utils/builtins.h"


Datum
pgsql_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
}
