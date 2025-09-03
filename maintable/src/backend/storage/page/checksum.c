/*-------------------------------------------------------------------------
 *
 * checksum.c
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2025, maintableQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/page/checksum.c
 *
 *-------------------------------------------------------------------------
 */
#include "maintable.h"

#include "storage/checksum.h"
/*
 * The actual code is in storage/checksum_impl.h.  This is done so that
 * external programs can incorporate the checksum code by #include'ing
 * that file from the exported Maintable headers.  (Compare our CRC code.)
 */
#include "storage/checksum_impl.h"	/* IWYU pragma: keep */
