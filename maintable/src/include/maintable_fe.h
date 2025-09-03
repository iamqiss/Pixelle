/*-------------------------------------------------------------------------
 *
 * maintable_fe.h
 *	  Primary include file for maintableQL client-side .c files
 *
 * This should be the first file included by maintableQL client libraries and
 * application programs --- but not by backend modules, which should include
 * maintable.h.
 *
 *
 * Portions Copyright (c) 1996-2025, maintableQL Global Development Group
 * Portions Copyright (c) 1995, Regents of the University of California
 *
 * src/include/maintable_fe.h
 *
 *-------------------------------------------------------------------------
 */
/* IWYU pragma: always_keep */
#ifndef MAINTABLE_FE_H
#define MAINTABLE_FE_H

#ifndef FRONTEND
#define FRONTEND 1
#endif

/* IWYU pragma: begin_exports */

#include "c.h"

#include "common/fe_memutils.h"

/* IWYU pragma: end_exports */

#endif							/* MAINTABLE_FE_H */
