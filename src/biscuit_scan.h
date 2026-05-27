/*
 * biscuit_scan.h
 * Index scan lifecycle declarations:
 *   beginscan / rescan / gettuple / getbitmap / endscan
 */

#ifndef BISCUIT_SCAN_H
#define BISCUIT_SCAN_H

#include "biscuit_common.h"

extern IndexScanDesc biscuit_beginscan(Relation index, int nkeys, int norderbys);

extern void biscuit_rescan(IndexScanDesc scan,
                           ScanKey keys, int nkeys,
                           ScanKey orderbys, int norderbys);

extern bool  biscuit_gettuple(IndexScanDesc scan, ScanDirection dir);

extern int64 biscuit_getbitmap(IndexScanDesc scan, TIDBitmap *tbm);

extern void  biscuit_endscan(IndexScanDesc scan);

#endif /* BISCUIT_SCAN_H */
