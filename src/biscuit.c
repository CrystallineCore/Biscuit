/*
 * biscuit_roaring.c
 * PostgreSQL extension with Roaring Bitmap + Biscuit CRUD + Optimized Pattern Matching
 * 
 * ARCHITECTURE:
 * - Biscuit's O(1) hash table PK lookup
 * - Biscuit's lazy deletion with tombstones
 * - Biscuit's incremental updates
 * - Optimized pattern matching with tombstone filtering
 */

 #include "postgres.h"
 #include "fmgr.h"
 #include "utils/builtins.h"
 #include "utils/memutils.h"
 #include "utils/hsearch.h"
 #include "access/htup_details.h"
 #include "catalog/pg_type.h"
 #include "funcapi.h"
 #include "executor/spi.h"
 #include "lib/stringinfo.h"
 #include "utils/timestamp.h"
 #include "commands/trigger.h"
 #include "executor/executor.h"
 #include "catalog/pg_type_d.h"     /* For type OIDs (UUIDOID, etc.) */
 #include "utils/syscache.h"        /* For getTypeOutputInfo */
 #include "utils/lsyscache.h"       /* For get_typlenbyvalalign, etc. */
 #include <string.h>
 #include "utils/uuid.h"
 #include "access/heapam.h"
 #include "access/table.h"
 #include "access/xact.h"
 #include "catalog/namespace.h"
 #include "utils/rel.h"
 #include "utils/snapmgr.h"
 #include "parser/parse_relation.h"
 #include "storage/bufmgr.h"
 #include "parser/parse_node.h"
 #include "nodes/nodes.h"
 #include "nodes/primnodes.h"
 #include "storage/block.h"
#include "storage/off.h"
#include "storage/itemptr.h"

 #ifdef HAVE_ROARING
 #include "roaring.h"
 #else
 typedef void roaring_bitmap_t;
 #endif
 
 #ifdef PG_MODULE_MAGIC 
 PG_MODULE_MAGIC;
 #endif

#define BISCUIT_VERSION "1.0.4-Biscuit"

PG_FUNCTION_INFO_V1(biscuit_version);
Datum biscuit_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(BISCUIT_VERSION));
}
 
 /* ==================== ROARING BITMAP WRAPPER ==================== */
 
 #ifdef HAVE_ROARING
 
 typedef roaring_bitmap_t RoaringBitmap;
 
 static inline RoaringBitmap* biscuit_roaring_create(void) { return roaring_bitmap_create(); }
 static inline void biscuit_roaring_add(RoaringBitmap *rb, uint32_t value) { roaring_bitmap_add(rb, value); }
 static inline void biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value) { roaring_bitmap_remove(rb, value); }
 static inline uint64_t biscuit_roaring_count(const RoaringBitmap *rb) { return roaring_bitmap_get_cardinality(rb); }
 static inline bool biscuit_roaring_is_empty(const RoaringBitmap *rb) { return roaring_bitmap_get_cardinality(rb) == 0; }
 static inline void biscuit_roaring_free(RoaringBitmap *rb) { if (rb) roaring_bitmap_free(rb); }
 static inline RoaringBitmap* biscuit_roaring_copy(const RoaringBitmap *rb) { return roaring_bitmap_copy(rb); }
 static inline void biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_and_inplace(a, b); }
 static inline void biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_or_inplace(a, b); }
 static inline void biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_andnot_inplace(a, b); }
 static inline size_t biscuit_roaring_size_bytes(const RoaringBitmap *rb) { return roaring_bitmap_size_in_bytes(rb); }

 static inline uint32_t* biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count)
 {
     uint32_t *array;
     *count = roaring_bitmap_get_cardinality(rb);
     if (*count == 0) return NULL;
     array = (uint32_t *)palloc(*count * sizeof(uint32_t));
     roaring_bitmap_to_uint32_array(rb, array);
     return array;
 }
 
 #else
 
 /* Fallback bitmap implementation */
 typedef struct {
     uint64_t *blocks;
     int num_blocks;
     int capacity;
     bool is_palloc;
 } RoaringBitmap;
 
 static inline RoaringBitmap* biscuit_roaring_create(void)
 {
     RoaringBitmap *rb = (RoaringBitmap *)palloc(sizeof(RoaringBitmap));
     rb->num_blocks = 0;
     rb->capacity = 16;
     rb->blocks = (uint64_t *)palloc0(rb->capacity * sizeof(uint64_t));
     rb->is_palloc = true;
     return rb;
 }
 
 static inline void biscuit_roaring_add(RoaringBitmap *rb, uint32_t value)
 {
     int block = value >> 6;
     int bit = value & 63;
     
     if (block >= rb->capacity)
     {
         int new_cap = (block + 1) * 2;
         uint64_t *new_blocks = (uint64_t *)palloc0(new_cap * sizeof(uint64_t));
         
         if (rb->num_blocks > 0)
             memcpy(new_blocks, rb->blocks, rb->num_blocks * sizeof(uint64_t));
         
         if (rb->is_palloc)
             pfree(rb->blocks);
         
         rb->blocks = new_blocks;
         rb->capacity = new_cap;
     }
     if (block >= rb->num_blocks)
         rb->num_blocks = block + 1;
     rb->blocks[block] |= (1ULL << bit);
 }
 
 static inline void biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value)
 {
     int block = value >> 6;
     int bit = value & 63;
     
     if (block < rb->num_blocks)
         rb->blocks[block] &= ~(1ULL << bit);
 }
 
 static inline uint64_t biscuit_roaring_count(const RoaringBitmap *rb)
 {
     uint64_t count = 0;
     int i;
     for (i = 0; i < rb->num_blocks; i++)
         count += __builtin_popcountll(rb->blocks[i]);
     return count;
 }
 
 static inline bool biscuit_roaring_is_empty(const RoaringBitmap *rb)
 {
     int i;
     for (i = 0; i < rb->num_blocks; i++)
         if (rb->blocks[i])
             return false;
     return true;
 }
 
 static inline uint32_t* biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count)
 {
     uint32_t *array;
     int idx = 0, i;
     uint64_t bits, base;
     
     *count = biscuit_roaring_count(rb);
     if (*count == 0)
         return NULL;
     
     array = (uint32_t *)palloc(*count * sizeof(uint32_t));
     
     for (i = 0; i < rb->num_blocks; i++)
     {
         bits = rb->blocks[i];
         if (!bits)
             continue;
         
         base = (uint64_t)i << 6;
         while (bits)
         {
             array[idx++] = (uint32_t)(base + __builtin_ctzll(bits));
             bits &= bits - 1;
         }
     }
     return array;
 }
 
 static inline size_t biscuit_roaring_size_bytes(const RoaringBitmap *rb)
 {
     return sizeof(RoaringBitmap) + rb->capacity * sizeof(uint64_t);
 }
 
 static inline void biscuit_roaring_free(RoaringBitmap *rb)
 {
     if (rb)
     {
         if (rb->blocks && rb->is_palloc)
             pfree(rb->blocks);
         pfree(rb);
     }
 }
 
 static inline RoaringBitmap* biscuit_roaring_copy(const RoaringBitmap *rb)
 {
     RoaringBitmap *copy = biscuit_roaring_create();
     
     if (rb->num_blocks > 0)
     {
         pfree(copy->blocks);
         copy->blocks = (uint64_t *)palloc(rb->num_blocks * sizeof(uint64_t));
         copy->num_blocks = rb->num_blocks;
         copy->capacity = rb->num_blocks;
         memcpy(copy->blocks, rb->blocks, rb->num_blocks * sizeof(uint64_t));
     }
     return copy;
 }
 
 static inline void biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b)
 {
     int min_blocks = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
     int i;
     
     for (i = 0; i < min_blocks; i++)
         a->blocks[i] &= b->blocks[i];
     
     for (i = min_blocks; i < a->num_blocks; i++)
         a->blocks[i] = 0;
     
     a->num_blocks = min_blocks;
 }
 
 static inline void biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b)
 {
     int i;
     
     if (b->num_blocks > a->capacity)
     {
         uint64_t *new_blocks = (uint64_t *)palloc0(b->num_blocks * sizeof(uint64_t));
         
         if (a->num_blocks > 0)
             memcpy(new_blocks, a->blocks, a->num_blocks * sizeof(uint64_t));
         
         if (a->is_palloc)
             pfree(a->blocks);
         
         a->blocks = new_blocks;
         a->capacity = b->num_blocks;
     }
     
     int min_blocks = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
     
     for (i = 0; i < min_blocks; i++)
         a->blocks[i] |= b->blocks[i];
     
     if (b->num_blocks > a->num_blocks)
     {
         memcpy(a->blocks + a->num_blocks, b->blocks + a->num_blocks,
                (b->num_blocks - a->num_blocks) * sizeof(uint64_t));
         a->num_blocks = b->num_blocks;
     }
 }
 
 static inline void biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b)
 {
     int min_blocks = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
     int i;
     
     for (i = 0; i < min_blocks; i++)
         a->blocks[i] &= ~b->blocks[i];
 }
 
 #endif
 
 /* ==================== INDEX STRUCTURES ==================== */
 
 #define MAX_POSITIONS 256
 #define CHAR_RANGE 256
 #define INITIAL_CAPACITY 64
 #define TOMBSTONE_CLEANUP_THRESHOLD 1000
 
 typedef struct {
     int pos;
     RoaringBitmap *bitmap;
 } PosEntry;
 
 typedef struct {
     PosEntry *entries;
     int count;
     int capacity;
 } CharIndex;
 
 typedef struct {
     RoaringBitmap **length_bitmaps;
     int max_length;
 } LengthIndex;
 
 typedef struct {
     uint32_t *free_indices;
     int count;
     int capacity;
 } FreeList;
 
 typedef struct {
    char pk_str[NAMEDATALEN];  // Fixed-size key for hash table
    uint32_t idx;
} PKMapEntry;
 
 typedef struct {
    RoaringBitmap *tombstones;
    uint32_t *deleted_indices;
    int tombstone_count;
    int tombstone_capacity;
    int tombstone_threshold;
    int64 total_cleanups;
    int64 items_cleaned;
} LazyDeletion;

typedef struct RoaringIndex {
    CharIndex pos_idx[CHAR_RANGE];
    CharIndex neg_idx[CHAR_RANGE];
    RoaringBitmap *char_cache[CHAR_RANGE];
    LengthIndex length_idx;
    
    char **data;
    char **primary_keys;
    int num_records;
    int capacity;
    int max_len;
    size_t memory_used;
    
    char *table_name;
    char *column_name;
    char *pk_column_name;
    
    FreeList free_list;
    HTAB *pk_to_index;
    LazyDeletion lazy_del;
    
    int64 insert_count;
    int64 update_count;
    int64 delete_count;
    int64 incremental_update_count;
    int64 query_count;
} RoaringIndex;

 static RoaringIndex *global_index = NULL;
 static MemoryContext index_context = NULL;
 
 /* ==================== FORWARD DECLARATIONS ==================== */
 static void biscuit_cleanup_tombstones(void);
 static RoaringBitmap* biscuit_get_pos_bitmap(unsigned char ch, int pos);
 static RoaringBitmap* biscuit_get_neg_bitmap(unsigned char ch, int neg_offset);
 static void biscuit_set_pos_bitmap(unsigned char ch, int pos, RoaringBitmap *bm);
 static void biscuit_set_neg_bitmap(unsigned char ch, int neg_offset, RoaringBitmap *bm);
 
 /* ==================== HASH TABLE & LAZY DELETION ==================== */
 
static void biscuit_init_pk_hash_table(void)
{
    HASHCTL ctl;
    
    MemSet(&ctl, 0, sizeof(ctl));
    
    /* Use HASH_STRINGS flag for C-string keys */
    ctl.keysize = NAMEDATALEN;        /* Maximum key length */
    ctl.entrysize = sizeof(PKMapEntry);
    ctl.hcxt = index_context;
    
    global_index->pk_to_index = hash_create(
        "Biscuit PK Map",
        global_index->capacity,
        &ctl,
        HASH_ELEM | HASH_STRINGS | HASH_CONTEXT
    );
}
 

 /* Convert any Datum to string representation for use as hash key */
 static char* biscuit_datum_to_pk_string(Datum pk_datum, Oid pk_type, bool *isnull)
 {
     char *result;
     Oid typoutput;
     bool typIsVarlena;
     char buffer[256];  /* Temp buffer for sprintf */
     
     if (*isnull)
         return NULL;
     
     /* Fast path for common integer types - USE PSTRDUP! */
     switch (pk_type)
     {
         case INT2OID:
             snprintf(buffer, sizeof(buffer), "%d", (int)DatumGetInt16(pk_datum));
             return pstrdup(buffer);
             
         case INT4OID:
             snprintf(buffer, sizeof(buffer), "%d", DatumGetInt32(pk_datum));
             return pstrdup(buffer);
             
         case INT8OID:
             snprintf(buffer, sizeof(buffer), "%lld", (long long)DatumGetInt64(pk_datum));
             return pstrdup(buffer);
             
         case TEXTOID:
         case VARCHAROID:
         case BPCHAROID:
             result = TextDatumGetCString(pk_datum);
             return result;
             
         case UUIDOID:
             /* Use PostgreSQL's standard UUID output function for consistency */
             getTypeOutputInfo(pk_type, &typoutput, &typIsVarlena);
             result = OidOutputFunctionCall(typoutput, pk_datum);
             return result;
     }
     
     /* Generic path: Use PostgreSQL's type output function */
     getTypeOutputInfo(pk_type, &typoutput, &typIsVarlena);
     result = OidOutputFunctionCall(typoutput, pk_datum);
     
     return result;
 }

 static int32_t biscuit_biscuit_find_index_by_pk_debug(const char *pk_str, bool log_details)
 {
     PKMapEntry *entry;
     bool found;
     char key[NAMEDATALEN];
     
     if (!global_index || !global_index->pk_to_index || !pk_str)
     {
         if (log_details)
             elog(NOTICE, "biscuit_find_index_by_pk: NULL check failed");
         return -1;
     }
     
     if (log_details)
         elog(NOTICE, "biscuit_find_index_by_pk: Looking up '%s'", pk_str);
     
     /* Create fixed-size key */
     MemSet(key, 0, NAMEDATALEN);
     strncpy(key, pk_str, NAMEDATALEN - 1);
     
     entry = (PKMapEntry *)hash_search(
         global_index->pk_to_index,
         key,
         HASH_FIND,
         &found
     );
     
     if (!found)
     {
         if (log_details)
             elog(NOTICE, "biscuit_find_index_by_pk: Key '%s' NOT FOUND in hash table", pk_str);
         return -1;
     }
     
     if (log_details)
         elog(NOTICE, "biscuit_find_index_by_pk: Found index %u for key '%s'", entry->idx, pk_str);
     
     /* Check tombstones */
     if (global_index->lazy_del.tombstones && 
         biscuit_roaring_count(global_index->lazy_del.tombstones) > 0)
     {
         uint32_t idx = entry->idx;
         #ifdef HAVE_ROARING
         if (roaring_bitmap_contains(global_index->lazy_del.tombstones, idx))
         {
             if (log_details)
                 elog(NOTICE, "biscuit_find_index_by_pk: Index %u is tombstoned", idx);
             return -1;
         }
         #else
         uint32_t block = idx >> 6;
         uint32_t bit = idx & 63;
         if (block < global_index->lazy_del.tombstones->num_blocks &&
             (global_index->lazy_del.tombstones->blocks[block] & (1ULL << bit)))
         {
             if (log_details)
                 elog(NOTICE, "biscuit_find_index_by_pk: Index %u is tombstoned", idx);
             return -1;
         }
         #endif
     }
     
     return entry->idx;
 }
static int32_t biscuit_find_index_by_pk(const char *pk_str)
{
    return biscuit_biscuit_find_index_by_pk_debug(pk_str, false);
}

static void biscuit_add_pk_mapping(const char *pk_str, uint32_t idx)
{
    PKMapEntry *entry;
    bool found;
    char key[NAMEDATALEN];
    
    if (!pk_str)
        ereport(ERROR, (errmsg("NULL primary key string")));
    
    /* Create fixed-size key */
    MemSet(key, 0, NAMEDATALEN);
    strncpy(key, pk_str, NAMEDATALEN - 1);
    
    entry = (PKMapEntry *)hash_search(
        global_index->pk_to_index,
        key,
        HASH_ENTER,
        &found
    );
    
    if (found)
    {
        //elog(WARNING, "biscuit_add_pk_mapping: Key '%s' already exists, updating index %u -> %u", pk_str, entry->idx, idx);
    }
    else
    {
        //elog(DEBUG1, "biscuit_add_pk_mapping: Added key '%s' -> index %u", pk_str, idx);
    }
    
    entry->idx = idx;
}

static void biscuit_remove_pk_mapping(const char *pk_str)
{
    char key[NAMEDATALEN];
    
    if (!pk_str)
        return;
    
    /* Create fixed-size key */
    MemSet(key, 0, NAMEDATALEN);
    strncpy(key, pk_str, NAMEDATALEN - 1);
    
    /* Remove from hash table */
    hash_search(
        global_index->pk_to_index,
        key,
        HASH_REMOVE,
        NULL
    );
}
 static void biscuit_init_lazy_deletion(void)
 {
     global_index->lazy_del.tombstones = biscuit_roaring_create();
     global_index->lazy_del.tombstone_count = 0;
     global_index->lazy_del.tombstone_threshold = TOMBSTONE_CLEANUP_THRESHOLD;
     global_index->lazy_del.tombstone_capacity = 64;
     global_index->lazy_del.deleted_indices = (uint32_t *)MemoryContextAlloc(
         index_context,
         global_index->lazy_del.tombstone_capacity * sizeof(uint32_t)
     );
     global_index->lazy_del.total_cleanups = 0;
     global_index->lazy_del.items_cleaned = 0;
 }
 
 static void biscuit_cleanup_tombstones(void)
{
    int ch, j, i, cleaned = 0;
    
    if (global_index->lazy_del.tombstone_count == 0)
        return;
    
    /* Free strings using stored indices */
    for (i = 0; i < global_index->lazy_del.tombstone_count; i++)
    {
        uint32_t idx = global_index->lazy_del.deleted_indices[i];
        if (global_index->data[idx])
        {
            pfree(global_index->data[idx]);
            global_index->data[idx] = NULL;
            cleaned++;
        }
    }
    
    /* Batch remove from all bitmaps */
    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        CharIndex *pos_cidx = &global_index->pos_idx[ch];
        for (j = 0; j < pos_cidx->count; j++)
            biscuit_roaring_andnot_inplace(pos_cidx->entries[j].bitmap, 
                                   global_index->lazy_del.tombstones);
        
        CharIndex *neg_cidx = &global_index->neg_idx[ch];
        for (j = 0; j < neg_cidx->count; j++)
            biscuit_roaring_andnot_inplace(neg_cidx->entries[j].bitmap,
                                   global_index->lazy_del.tombstones);
        
        if (global_index->char_cache[ch])
            biscuit_roaring_andnot_inplace(global_index->char_cache[ch],
                                   global_index->lazy_del.tombstones);
    }
    
    /* Cleanup length index */
    for (j = 0; j < global_index->length_idx.max_length; j++)
    {
        if (global_index->length_idx.length_bitmaps[j])
            biscuit_roaring_andnot_inplace(global_index->length_idx.length_bitmaps[j],
                                   global_index->lazy_del.tombstones);
    }
    
    global_index->lazy_del.items_cleaned += cleaned;
    global_index->lazy_del.total_cleanups++;
    
    biscuit_roaring_free(global_index->lazy_del.tombstones);
    global_index->lazy_del.tombstones = biscuit_roaring_create();
    global_index->lazy_del.tombstone_count = 0;
}
 
 /* ==================== FREE LIST ==================== */
 
 static void biscuit_init_free_list(FreeList *fl)
 {
     fl->capacity = 64;
     fl->count = 0;
     fl->free_indices = (uint32_t *)MemoryContextAlloc(index_context, 
                                                        fl->capacity * sizeof(uint32_t));
 }
 
 static void biscuit_push_free_index(FreeList *fl, uint32_t idx)
 {
     if (fl->count >= fl->capacity)
     {
         int new_cap = fl->capacity * 2;
         uint32_t *new_indices = (uint32_t *)MemoryContextAlloc(index_context, 
                                                                 new_cap * sizeof(uint32_t));
         memcpy(new_indices, fl->free_indices, fl->count * sizeof(uint32_t));
         fl->free_indices = new_indices;
         fl->capacity = new_cap;
     }
     fl->free_indices[fl->count++] = idx;
 }
 
 static bool biscuit_pop_free_index(FreeList *fl, uint32_t *idx)
 {
     if (fl->count == 0)
         return false;
     *idx = fl->free_indices[--fl->count];
     return true;
 }
 
 /* ==================== BITMAP ACCESS FUNCTIONS ==================== */
 
 static inline RoaringBitmap* biscuit_get_pos_bitmap(unsigned char ch, int pos)
 {
     CharIndex *cidx = &global_index->pos_idx[ch];
     int left = 0, right = cidx->count - 1;
     
     while (left <= right)
     {
         int mid = (left + right) >> 1;
         if (cidx->entries[mid].pos == pos)
             return cidx->entries[mid].bitmap;
         else if (cidx->entries[mid].pos < pos)
             left = mid + 1;
         else
             right = mid - 1;
     }
     
     return NULL;
 }
 
 static inline RoaringBitmap* biscuit_get_neg_bitmap(unsigned char ch, int neg_offset)
 {
     CharIndex *cidx = &global_index->neg_idx[ch];
     int left = 0, right = cidx->count - 1;
     
     while (left <= right)
     {
         int mid = (left + right) >> 1;
         if (cidx->entries[mid].pos == neg_offset)
             return cidx->entries[mid].bitmap;
         else if (cidx->entries[mid].pos < neg_offset)
             left = mid + 1;
         else
             right = mid - 1;
     }
     
     return NULL;
 }
 
 static void biscuit_set_pos_bitmap(unsigned char ch, int pos, RoaringBitmap *bm)
 {
     CharIndex *cidx = &global_index->pos_idx[ch];
     int i, left = 0, right = cidx->count - 1;
     int insert_pos = cidx->count;
     
     while (left <= right)
     {
         int mid = (left + right) >> 1;
         if (cidx->entries[mid].pos == pos)
         {
             cidx->entries[mid].bitmap = bm;
             return;
         }
         else if (cidx->entries[mid].pos < pos)
             left = mid + 1;
         else
         {
             insert_pos = mid;
             right = mid - 1;
         }
     }
     
     if (cidx->count >= cidx->capacity)
     {
         int new_cap = cidx->capacity * 2;
         PosEntry *new_entries = (PosEntry *)MemoryContextAlloc(index_context, new_cap * sizeof(PosEntry));
         if (cidx->count > 0)
             memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
         cidx->entries = new_entries;
         cidx->capacity = new_cap;
     }
     
     for (i = cidx->count; i > insert_pos; i--)
         cidx->entries[i] = cidx->entries[i - 1];
     
     cidx->entries[insert_pos].pos = pos;
     cidx->entries[insert_pos].bitmap = bm;
     cidx->count++;
 }
 
 static void biscuit_set_neg_bitmap(unsigned char ch, int neg_offset, RoaringBitmap *bm)
 {
     CharIndex *cidx = &global_index->neg_idx[ch];
     int i, left = 0, right = cidx->count - 1;
     int insert_pos = cidx->count;
     
     while (left <= right)
     {
         int mid = (left + right) >> 1;
         if (cidx->entries[mid].pos == neg_offset)
         {
             cidx->entries[mid].bitmap = bm;
             return;
         }
         else if (cidx->entries[mid].pos < neg_offset)
             left = mid + 1;
         else
         {
             insert_pos = mid;
             right = mid - 1;
         }
     }
     
     if (cidx->count >= cidx->capacity)
     {
         int new_cap = cidx->capacity * 2;
         PosEntry *new_entries = (PosEntry *)MemoryContextAlloc(index_context, new_cap * sizeof(PosEntry));
         if (cidx->count > 0)
             memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
         cidx->entries = new_entries;
         cidx->capacity = new_cap;
     }
     
     for (i = cidx->count; i > insert_pos; i--)
         cidx->entries[i] = cidx->entries[i - 1];
     
     cidx->entries[insert_pos].pos = neg_offset;
     cidx->entries[insert_pos].bitmap = bm;
     cidx->count++;
 }
 
 /* ==================== PATTERN MATCHING UTILITY FUNCTIONS ==================== */
 
 static RoaringBitmap* biscuit_get_length_ge(int min_len)
 {
     RoaringBitmap *result = biscuit_roaring_create();
     int len;
     
     for (len = min_len; len < global_index->length_idx.max_length; len++)
     {
         if (global_index->length_idx.length_bitmaps[len])
         {
             RoaringBitmap *filtered = biscuit_roaring_copy(global_index->length_idx.length_bitmaps[len]);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(filtered, global_index->lazy_del.tombstones);
             
             biscuit_roaring_or_inplace(result, filtered);
             biscuit_roaring_free(filtered);
         }
     }
     
     return result;
 }
 
 static RoaringBitmap* biscuit_get_any_char_at_pos(int pos)
 {
     RoaringBitmap *result = biscuit_roaring_create();
     int ch;
     
     for (ch = 0; ch < CHAR_RANGE; ch++)
     {
         RoaringBitmap *char_bm = biscuit_get_pos_bitmap((unsigned char)ch, pos);
         if (char_bm)
         {
             RoaringBitmap *filtered = biscuit_roaring_copy(char_bm);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(filtered, global_index->lazy_del.tombstones);
             
             biscuit_roaring_or_inplace(result, filtered);
             biscuit_roaring_free(filtered);
         }
     }
     
     return result;
 }
 
 static RoaringBitmap* biscuit_get_any_char_at_neg(int neg_pos)
 {
     RoaringBitmap *result = biscuit_roaring_create();
     int ch;
     
     for (ch = 0; ch < CHAR_RANGE; ch++)
     {
         RoaringBitmap *char_bm = biscuit_get_neg_bitmap((unsigned char)ch, neg_pos);
         if (char_bm)
         {
             RoaringBitmap *filtered = biscuit_roaring_copy(char_bm);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(filtered, global_index->lazy_del.tombstones);
             
             biscuit_roaring_or_inplace(result, filtered);
             biscuit_roaring_free(filtered);
         }
     }
     
     return result;
 }
 
 static RoaringBitmap* biscuit_match_part_at_pos(const char *part, int part_len, int start_pos)
 {
     RoaringBitmap *result = NULL;
     int i;
     
     for (i = 0; i < part_len; i++)
     {
         RoaringBitmap *char_bm;
         
         if (part[i] == '_')
         {
             char_bm = biscuit_get_any_char_at_pos(start_pos + i);
         }
         else
         {
             char_bm = biscuit_get_pos_bitmap((unsigned char)part[i], start_pos + i);
             if (!char_bm)
             {
                 if (result)
                     biscuit_roaring_free(result);
                 return biscuit_roaring_create();
             }
             char_bm = biscuit_roaring_copy(char_bm);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(char_bm, global_index->lazy_del.tombstones);
         }
         
         if (!result)
             result = char_bm;
         else
         {
             biscuit_roaring_and_inplace(result, char_bm);
             biscuit_roaring_free(char_bm);
             if (biscuit_roaring_is_empty(result))
                 return result;
         }
     }
     
     return result ? result : biscuit_roaring_create();
 }
 
 static RoaringBitmap* biscuit_match_part_at_end(const char *part, int part_len)
 {
     RoaringBitmap *result = NULL;
     int i;
     
     for (i = 0; i < part_len; i++)
     {
         int neg_pos = -(part_len - i);
         RoaringBitmap *char_bm;
         
         if (part[i] == '_')
         {
             char_bm = biscuit_get_any_char_at_neg(neg_pos);
         }
         else
         {
             char_bm = biscuit_get_neg_bitmap((unsigned char)part[i], neg_pos);
             if (!char_bm)
             {
                 if (result)
                     biscuit_roaring_free(result);
                 return biscuit_roaring_create();
             }
             char_bm = biscuit_roaring_copy(char_bm);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(char_bm, global_index->lazy_del.tombstones);
         }
         
         if (!result)
             result = char_bm;
         else
         {
             biscuit_roaring_and_inplace(result, char_bm);
             biscuit_roaring_free(char_bm);
             if (biscuit_roaring_is_empty(result))
                 return result;
         }
     }
     
     return result ? result : biscuit_roaring_create();
 }
 
 static RoaringBitmap* biscuit_match_part_anywhere(const char *part, int part_len)
 {
     RoaringBitmap *result = biscuit_roaring_create();
     int pos;
     
     for (pos = 0; pos <= global_index->max_len - part_len; pos++)
     {
         RoaringBitmap *match = biscuit_match_part_at_pos(part, part_len, pos);
         if (!biscuit_roaring_is_empty(match))
             biscuit_roaring_or_inplace(result, match);
         biscuit_roaring_free(match);
     }
     
     return result;
 }
 
 static void biscuit_recursive_windowed_match(
    RoaringBitmap *result,
    const char **parts,
    int *part_lens,
    int part_count,
    bool ends_percent,
    int part_idx,
    int min_pos,
    RoaringBitmap *current_candidates,
    int max_len
)
{
    int i, pos;
    
    if (part_idx >= part_count)
    {
        biscuit_roaring_or_inplace(result, current_candidates);
        return;
    }
    
    /* Calculate remaining required length for all future parts */
    int remaining_len = 0;
    for (i = part_idx + 1; i < part_count; i++)
        remaining_len += part_lens[i];
    
    /* Special handling for last part without trailing % */
    if (part_idx == part_count - 1 && !ends_percent)
    {
        /* CRITICAL FIX: Filter candidates to ensure the last part can fit
         * WITHOUT overlapping with where current parts have been matched */
        RoaringBitmap *last_match = biscuit_match_part_at_end(parts[part_idx], part_lens[part_idx]);
        biscuit_roaring_and_inplace(last_match, current_candidates);
        
        /* ADDITIONAL CHECK: Ensure string is long enough
         * min_pos indicates where we are, and we need part_lens[part_idx] more characters */
        uint64_t count = 0;
        uint32_t *indices = biscuit_roaring_to_array(last_match, &count);
        RoaringBitmap *filtered = biscuit_roaring_create();
        
        for (i = 0; i < count; i++)
        {
            uint32_t idx = indices[i];
            int str_len = strlen(global_index->data[idx]);
            
            /* Check if there's room: min_pos + part_lens[part_idx] <= str_len */
            if (min_pos + part_lens[part_idx] <= str_len)
            {
                biscuit_roaring_add(filtered, idx);
            }
        }
        
        if (indices)
            pfree(indices);
        
        biscuit_roaring_or_inplace(result, filtered);
        biscuit_roaring_free(filtered);
        biscuit_roaring_free(last_match);
        return;
    }
    
    /* Calculate max position for current part */
    int max_pos = max_len - part_lens[part_idx] - remaining_len;
    
    /* Ensure we don't exceed valid range */
    if (min_pos > max_pos)
        return;
    
    /* Try matching this part at each valid position */
    for (pos = min_pos; pos <= max_pos; pos++)
    {
        RoaringBitmap *part_at_pos = biscuit_match_part_at_pos(parts[part_idx], part_lens[part_idx], pos);
        
        biscuit_roaring_and_inplace(part_at_pos, current_candidates);
        
        if (!biscuit_roaring_is_empty(part_at_pos))
        {
            /* Next part must start AFTER this part ends */
            biscuit_recursive_windowed_match(result, parts, part_lens, part_count, ends_percent,
                                    part_idx + 1, pos + part_lens[part_idx], part_at_pos, max_len);
        }
        
        biscuit_roaring_free(part_at_pos);
    }
}

static RoaringBitmap* biscuit_match_multipart_windowed_positions(
    const char **parts, 
    int *part_lens, 
    int part_count,
    bool starts_percent,
    bool ends_percent
)
{
    RoaringBitmap *result = biscuit_roaring_create();
    int min_len = 0;
    int i;
    
    /* Calculate total minimum length */
    for (i = 0; i < part_count; i++)
        min_len += part_lens[i];
    
    /* Single part optimization */
    if (part_count == 1)
    {
        RoaringBitmap *match, *length_filter;
        
        if (!starts_percent && !ends_percent)
        {
            /* Exact match */
            match = biscuit_match_part_at_pos(parts[0], part_lens[0], 0);
            length_filter = (part_lens[0] < global_index->length_idx.max_length) ?
                biscuit_roaring_copy(global_index->length_idx.length_bitmaps[part_lens[0]]) : biscuit_roaring_create();
            
            if (global_index->lazy_del.tombstone_count > 0)
                biscuit_roaring_andnot_inplace(length_filter, global_index->lazy_del.tombstones);
            
            biscuit_roaring_and_inplace(match, length_filter);
            biscuit_roaring_free(length_filter);
            return match;
        }
        else if (!starts_percent)
        {
            /* Prefix match */
            match = biscuit_match_part_at_pos(parts[0], part_lens[0], 0);
            length_filter = biscuit_get_length_ge(part_lens[0]);
            biscuit_roaring_and_inplace(match, length_filter);
            biscuit_roaring_free(length_filter);
            return match;
        }
        else if (!ends_percent)
        {
            /* Suffix match */
            match = biscuit_match_part_at_end(parts[0], part_lens[0]);
            length_filter = biscuit_get_length_ge(part_lens[0]);
            biscuit_roaring_and_inplace(match, length_filter);
            biscuit_roaring_free(length_filter);
            return match;
        }
        else
        {
            /* Contains match */
            return biscuit_match_part_anywhere(parts[0], part_lens[0]);
        }
    }
    
    /* Two-part optimization for prefix%suffix */
    if (part_count == 2 && !starts_percent && !ends_percent)
    {
        RoaringBitmap *prefix = biscuit_match_part_at_pos(parts[0], part_lens[0], 0);
        RoaringBitmap *suffix = biscuit_match_part_at_end(parts[1], part_lens[1]);
        RoaringBitmap *length_filter = biscuit_get_length_ge(min_len);
        
        biscuit_roaring_and_inplace(prefix, suffix);
        biscuit_roaring_and_inplace(prefix, length_filter);
        biscuit_roaring_free(suffix);
        biscuit_roaring_free(length_filter);
        return prefix;
    }
    
    /* General multipart case */
    RoaringBitmap *initial_candidates = biscuit_get_length_ge(min_len);
    
    if (biscuit_roaring_is_empty(initial_candidates))
    {
        biscuit_roaring_free(initial_candidates);
        return result;
    }
    
    if (!starts_percent)
    {
        /* First part must be at position 0 */
        RoaringBitmap *first_match = biscuit_match_part_at_pos(parts[0], part_lens[0], 0);
        biscuit_roaring_and_inplace(initial_candidates, first_match);
        biscuit_roaring_free(first_match);
        
        if (biscuit_roaring_is_empty(initial_candidates))
        {
            biscuit_roaring_free(initial_candidates);
            return result;
        }
        
        /* FIXED: Use global max_len, the recursion will handle per-string validation */
        biscuit_recursive_windowed_match(result, parts, part_lens, part_count, ends_percent,
                                1, part_lens[0], initial_candidates, global_index->max_len);
    }
    else
    {
        /* FIXED: Use global max_len, the recursion will handle per-string validation */
        biscuit_recursive_windowed_match(result, parts, part_lens, part_count, ends_percent,
                                0, 0, initial_candidates, global_index->max_len);
    }
    
    biscuit_roaring_free(initial_candidates);
    return result;
} 
 /* ==================== PATTERN PARSING ==================== */
 
 typedef struct {
     char **parts;
     int *part_lens;
     int part_count;
     bool starts_percent;
     bool ends_percent;
 } ParsedPattern;
 
 static ParsedPattern* biscuit_parse_pattern(const char *pattern)
 {
     ParsedPattern *parsed = (ParsedPattern *)palloc(sizeof(ParsedPattern));
     int plen = strlen(pattern);
     int i, part_start = 0;
     int part_cap = 8;
     
     parsed->parts = (char **)palloc(part_cap * sizeof(char *));
     parsed->part_lens = (int *)palloc(part_cap * sizeof(int));
     parsed->part_count = 0;
     parsed->starts_percent = (plen > 0 && pattern[0] == '%');
     parsed->ends_percent = (plen > 0 && pattern[plen - 1] == '%');
     
     if (parsed->starts_percent)
         part_start = 1;
     
     for (i = part_start; i < plen; i++)
     {
         if (pattern[i] == '%')
         {
             int part_len = i - part_start;
             if (part_len > 0)
             {
                 if (parsed->part_count >= part_cap)
                 {
                     int new_cap = part_cap * 2;
                     char **new_parts = (char **)palloc(new_cap * sizeof(char *));
                     int *new_lens = (int *)palloc(new_cap * sizeof(int));
                     
                     memcpy(new_parts, parsed->parts, part_cap * sizeof(char *));
                     memcpy(new_lens, parsed->part_lens, part_cap * sizeof(int));
                     
                     pfree(parsed->parts);
                     pfree(parsed->part_lens);
                     
                     parsed->parts = new_parts;
                     parsed->part_lens = new_lens;
                     part_cap = new_cap;
                 }
                 parsed->parts[parsed->part_count] = pnstrdup(pattern + part_start, part_len);
                 parsed->part_lens[parsed->part_count] = part_len;
                 parsed->part_count++;
             }
             part_start = i + 1;
         }
     }
     
     if (part_start < plen && (!parsed->ends_percent || part_start < plen - 1))
     {
         int part_len = (parsed->ends_percent) ? (plen - 1 - part_start) : (plen - part_start);
         if (part_len > 0)
         {
             if (parsed->part_count >= part_cap)
             {
                 int new_cap = part_cap * 2;
                 char **new_parts = (char **)palloc(new_cap * sizeof(char *));
                 int *new_lens = (int *)palloc(new_cap * sizeof(int));
                 
                 memcpy(new_parts, parsed->parts, part_cap * sizeof(char *));
                 memcpy(new_lens, parsed->part_lens, part_cap * sizeof(int));
                 
                 pfree(parsed->parts);
                 pfree(parsed->part_lens);
                 
                 parsed->parts = new_parts;
                 parsed->part_lens = new_lens;
                 part_cap = new_cap;
             }
             parsed->parts[parsed->part_count] = pnstrdup(pattern + part_start, part_len);
             parsed->part_lens[parsed->part_count] = part_len;
             parsed->part_count++;
         }
     }
     
     return parsed;
 }
 
 static void biscuit_free_parsed_pattern(ParsedPattern *parsed)
 {
     int i;
     for (i = 0; i < parsed->part_count; i++)
         pfree(parsed->parts[i]);
     pfree(parsed->parts);
     pfree(parsed->part_lens);
     pfree(parsed);
 }
 
 /* ==================== FAST PATH OPTIMIZATIONS ==================== */
 
 static bool biscuit_is_wildcard_only(const char *pattern, int plen, int *underscore_count)
 {
     int i, count = 0;
     for (i = 0; i < plen; i++)
     {
         if (pattern[i] == '_')
             count++;
         else if (pattern[i] != '%')
             return false;
     }
     *underscore_count = count;
     return true;
 }
 
 static RoaringBitmap* biscuit_match_wildcard_only(const char *pattern, int plen, int underscore_count)
 {
     bool has_percent = false;
     int i;
     
     for (i = 0; i < plen; i++)
     {
         if (pattern[i] == '%')
         {
             has_percent = true;
             break;
         }
     }
     
     if (!has_percent)
     {
         if (underscore_count < global_index->length_idx.max_length &&
             global_index->length_idx.length_bitmaps[underscore_count])
         {
             RoaringBitmap *result = biscuit_roaring_copy(global_index->length_idx.length_bitmaps[underscore_count]);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
             
             return result;
         }
         return biscuit_roaring_create();
     }
     
     return biscuit_get_length_ge(underscore_count);
 }
 
 static RoaringBitmap* biscuit_match_simple_positioned(const char *pattern, int plen)
 {
     int i, first_char_pos = -1;
     char first_char = '\0';
     bool has_percent_start = false, has_percent_end = false;
     int char_count = 0;
     
     for (i = 0; i < plen; i++)
     {
         if (pattern[i] == '_')
             return NULL;
         else if (pattern[i] != '%')
             char_count++;
     }
     
     if (char_count != 1)
         return NULL;
     
     if (plen > 0 && pattern[0] == '%')
         has_percent_start = true;
     
     for (i = 0; i < plen; i++)
     {
         if (pattern[i] != '%')
         {
             first_char = pattern[i];
             first_char_pos = i;
             break;
         }
     }
     
     if (first_char_pos == -1)
         return NULL;
     
     if (plen > 0 && pattern[plen - 1] == '%')
         has_percent_end = true;
     
     bool prefix_pattern = !has_percent_start && has_percent_end;
     bool suffix_pattern = has_percent_start && !has_percent_end;
     bool contains_pattern = has_percent_start && has_percent_end;
     bool exact_pattern = !has_percent_start && !has_percent_end;
     
     if (prefix_pattern)
     {
         RoaringBitmap *char_bm = biscuit_get_pos_bitmap((unsigned char)first_char, 0);
         if (!char_bm)
             return biscuit_roaring_create();
         
         RoaringBitmap *result = biscuit_roaring_copy(char_bm);
         
         /* Filter tombstones */
         if (global_index->lazy_del.tombstone_count > 0)
             biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
         
         RoaringBitmap *length_filter = biscuit_get_length_ge(1);
         biscuit_roaring_and_inplace(result, length_filter);
         biscuit_roaring_free(length_filter);
         return result;
     }
     
     if (suffix_pattern)
     {
         RoaringBitmap *char_bm = biscuit_get_neg_bitmap((unsigned char)first_char, -1);
         if (!char_bm)
             return biscuit_roaring_create();
         
         RoaringBitmap *result = biscuit_roaring_copy(char_bm);
         
         /* Filter tombstones */
         if (global_index->lazy_del.tombstone_count > 0)
             biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
         
         RoaringBitmap *length_filter = biscuit_get_length_ge(1);
         biscuit_roaring_and_inplace(result, length_filter);
         biscuit_roaring_free(length_filter);
         return result;
     }
     
     if (contains_pattern)
     {
         if (global_index->char_cache[(unsigned char)first_char])
         {
             RoaringBitmap *result = biscuit_roaring_copy(global_index->char_cache[(unsigned char)first_char]);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
             
             return result;
         }
         return biscuit_roaring_create();
     }
     
     if (exact_pattern)
     {
         RoaringBitmap *char_bm = biscuit_get_pos_bitmap((unsigned char)first_char, 0);
         if (!char_bm)
             return biscuit_roaring_create();
         
         RoaringBitmap *result = biscuit_roaring_copy(char_bm);
         
         /* Filter tombstones */
         if (global_index->lazy_del.tombstone_count > 0)
             biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
         
         if (1 < global_index->length_idx.max_length && 
             global_index->length_idx.length_bitmaps[1])
         {
             RoaringBitmap *length_bm = biscuit_roaring_copy(global_index->length_idx.length_bitmaps[1]);
             
             /* Filter tombstones from length bitmap */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(length_bm, global_index->lazy_del.tombstones);
             
             biscuit_roaring_and_inplace(result, length_bm);
             biscuit_roaring_free(length_bm);
         }
         else
         {
             biscuit_roaring_free(result);
             return biscuit_roaring_create();
         }
         
         return result;
     }
     
     return NULL;
 }
 
 /* ==================== BISCUIT: CRUD OPERATIONS ==================== */
 
static void biscuit_add_to_index(uint32_t idx, const char *str)
{
    int len = strlen(str);
    int pos, i;
    RoaringBitmap *existing_bm;
    
    /* Handle empty string case */
    if (len == 0)
    {
        /* Ensure length bitmap for 0 exists */
        if (0 >= global_index->length_idx.max_length)
        {
            int old_max = global_index->length_idx.max_length;
            int new_max = 1;
            RoaringBitmap **new_bitmaps = (RoaringBitmap **)MemoryContextAlloc(
                index_context,
                new_max * sizeof(RoaringBitmap *)
            );
            
            if (old_max > 0)
                memcpy(new_bitmaps, global_index->length_idx.length_bitmaps,
                       old_max * sizeof(RoaringBitmap *));
            
            for (i = old_max; i < new_max; i++)
                new_bitmaps[i] = NULL;
            
            global_index->length_idx.length_bitmaps = new_bitmaps;
            global_index->length_idx.max_length = new_max;
        }
        
        if (!global_index->length_idx.length_bitmaps[0])
            global_index->length_idx.length_bitmaps[0] = biscuit_roaring_create();
        biscuit_roaring_add(global_index->length_idx.length_bitmaps[0], idx);
        
        return;
    }
    
    if (len > MAX_POSITIONS)
        len = MAX_POSITIONS;
    
    /* Index all character positions */
    for (pos = 0; pos < len; pos++)
    {
        unsigned char uch = (unsigned char)str[pos];
        
        /* Positive position index */
        existing_bm = biscuit_get_pos_bitmap(uch, pos);
        if (!existing_bm)
        {
            existing_bm = biscuit_roaring_create();
            biscuit_set_pos_bitmap(uch, pos, existing_bm);
        }
        biscuit_roaring_add(existing_bm, idx);
        
        /* Negative position index (from end) */
        int neg_offset = -(len - pos);
        existing_bm = biscuit_get_neg_bitmap(uch, neg_offset);
        if (!existing_bm)
        {
            existing_bm = biscuit_roaring_create();
            biscuit_set_neg_bitmap(uch, neg_offset, existing_bm);
        }
        biscuit_roaring_add(existing_bm, idx);
        
        /* Character cache */
        if (!global_index->char_cache[uch])
            global_index->char_cache[uch] = biscuit_roaring_create();
        biscuit_roaring_add(global_index->char_cache[uch], idx);
    }
    
    /* Add to length index */
    if (len >= global_index->length_idx.max_length)
    {
        int old_max = global_index->length_idx.max_length;
        int new_max = len + 1;
        RoaringBitmap **new_bitmaps = (RoaringBitmap **)MemoryContextAlloc(
            index_context,
            new_max * sizeof(RoaringBitmap *)
        );
        
        if (old_max > 0)
            memcpy(new_bitmaps, global_index->length_idx.length_bitmaps,
                   old_max * sizeof(RoaringBitmap *));
        
        for (i = old_max; i < new_max; i++)
            new_bitmaps[i] = NULL;
        
        global_index->length_idx.length_bitmaps = new_bitmaps;
        global_index->length_idx.max_length = new_max;
    }
    
    if (!global_index->length_idx.length_bitmaps[len])
        global_index->length_idx.length_bitmaps[len] = biscuit_roaring_create();
    biscuit_roaring_add(global_index->length_idx.length_bitmaps[len], idx);
    
    if (len > global_index->max_len)
        global_index->max_len = len;
}
 /* Incremental update for similar strings */
static bool biscuit_update_incremental(uint32_t idx, const char *old_value, const char *new_value)
{
    int old_len = strlen(old_value);
    int new_len = strlen(new_value);
    int i, diff_count = 0;
    int diff_positions[MAX_POSITIONS];
    RoaringBitmap *bm;
    
    /* Only works for same-length strings */
    if (old_len != new_len)
        return false;
    
    /* Don't use incremental for very short strings */
    if (old_len < 3)
        return false;
    
    if (old_len > MAX_POSITIONS)
        old_len = MAX_POSITIONS;
    
    /* Count differences */
    for (i = 0; i < old_len; i++)
    {
        if (old_value[i] != new_value[i])
        {
            if (diff_count >= MAX_POSITIONS)
                return false;
            diff_positions[diff_count++] = i;
        }
    }
    
    /* No changes - nothing to do */
    if (diff_count == 0)
        return true;
    
    /* Only use incremental update if < 20% changed AND at most 3 chars changed */
    if (diff_count > 3 || diff_count >= old_len / 5)
        return false;
    
    /* Apply incremental changes */
    for (i = 0; i < diff_count; i++)
    {
        int pos = diff_positions[i];
        unsigned char old_ch = (unsigned char)old_value[pos];
        unsigned char new_ch = (unsigned char)new_value[pos];
        int neg_offset = -(old_len - pos);
        
        /* Remove old character at position */
        bm = biscuit_get_pos_bitmap(old_ch, pos);
        if (bm) biscuit_roaring_remove(bm, idx);
        
        bm = biscuit_get_neg_bitmap(old_ch, neg_offset);
        if (bm) biscuit_roaring_remove(bm, idx);
        
        /* Update char cache - only remove if char doesn't appear elsewhere */
        if (global_index->char_cache[old_ch])
        {
            bool found_elsewhere = false;
            int j;
            for (j = 0; j < old_len; j++)
            {
                if (j != pos && old_value[j] == old_ch)
                {
                    found_elsewhere = true;
                    break;
                }
            }
            if (!found_elsewhere)
                biscuit_roaring_remove(global_index->char_cache[old_ch], idx);
        }
        
        /* Add new character at position */
        bm = biscuit_get_pos_bitmap(new_ch, pos);
        if (!bm)
        {
            bm = biscuit_roaring_create();
            biscuit_set_pos_bitmap(new_ch, pos, bm);
        }
        biscuit_roaring_add(bm, idx);
        
        bm = biscuit_get_neg_bitmap(new_ch, neg_offset);
        if (!bm)
        {
            bm = biscuit_roaring_create();
            biscuit_set_neg_bitmap(new_ch, neg_offset, bm);
        }
        biscuit_roaring_add(bm, idx);
        
        if (!global_index->char_cache[new_ch])
            global_index->char_cache[new_ch] = biscuit_roaring_create();
        biscuit_roaring_add(global_index->char_cache[new_ch], idx);
    }
    
    global_index->incremental_update_count++;
    return true;
}
 
static void biscuit_resurrect_slot(uint32_t idx)
{
    int i;
    
    /* Remove from tombstone bitmap */
    biscuit_roaring_remove(global_index->lazy_del.tombstones, idx);
    
    /* Remove from deleted_indices array */
    for (i = 0; i < global_index->lazy_del.tombstone_count; i++)
    {
        if (global_index->lazy_del.deleted_indices[i] == idx)
        {
            /* Shift remaining elements left */
            if (i < global_index->lazy_del.tombstone_count - 1)
            {
                memmove(&global_index->lazy_del.deleted_indices[i],
                       &global_index->lazy_del.deleted_indices[i + 1],
                       (global_index->lazy_del.tombstone_count - i - 1) * sizeof(uint32_t));
            }
            global_index->lazy_del.tombstone_count--;
            break;
        }
    }
}

static void biscuit_remove_from_all_indices(uint32_t idx)
{
    int ch, j;
    RoaringBitmap *bm;
    
    /* Remove from ALL position bitmaps (both positive and negative) */
    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        CharIndex *pos_cidx = &global_index->pos_idx[ch];
        for (j = 0; j < pos_cidx->count; j++)
        {
            bm = pos_cidx->entries[j].bitmap;
            if (bm)
                biscuit_roaring_remove(bm, idx);
        }
        
        CharIndex *neg_cidx = &global_index->neg_idx[ch];
        for (j = 0; j < neg_cidx->count; j++)
        {
            bm = neg_cidx->entries[j].bitmap;
            if (bm)
                biscuit_roaring_remove(bm, idx);
        }
        
        /* Remove from character cache */
        if (global_index->char_cache[ch])
            biscuit_roaring_remove(global_index->char_cache[ch], idx);
    }
    
    /* Remove from ALL length bitmaps */
    for (j = 0; j < global_index->length_idx.max_length; j++)
    {
        if (global_index->length_idx.length_bitmaps[j])
            biscuit_roaring_remove(global_index->length_idx.length_bitmaps[j], idx);
    }
}


static void biscuit_index_update(const char *pk_str, const char *new_value);

static void biscuit_index_insert(const char *pk_str, const char *value)
{
    uint32_t idx;
    char *value_copy;
    char *pk_copy;
    MemoryContext oldcontext;
    int32_t existing_idx;
    
    if (!global_index)
        ereport(ERROR, (errmsg("Index not initialized")));
    
    if (!pk_str)
        ereport(ERROR, (errmsg("NULL primary key")));
    
    //elog(DEBUG1, "biscuit_index_insert: Called for PK='%s', value='%s'", pk_str, value);
    
    /* CHECK: Does this PK already exist? */
    existing_idx = biscuit_find_index_by_pk(pk_str);
    if (existing_idx >= 0)
    {
        //elog(WARNING, "biscuit_index_insert: PK '%s' already exists at index %d, converting to UPDATE", pk_str, existing_idx);
        biscuit_index_update(pk_str, value);
        return;
    }
    
    oldcontext = MemoryContextSwitchTo(index_context);
    
    /* Try to reuse a deleted index */
    if (biscuit_pop_free_index(&global_index->free_list, &idx))
    {
        //elog(DEBUG1, "biscuit_index_insert: Reusing slot %u", idx);
        
        /* CRITICAL FIX: Remove from tombstone tracking */
        biscuit_resurrect_slot(idx);
        biscuit_remove_from_all_indices(idx);
        /* Reuse existing slot */
        if (global_index->data[idx])
            pfree(global_index->data[idx]);
        if (global_index->primary_keys[idx])
            pfree(global_index->primary_keys[idx]);
        
        value_copy = pstrdup(value);
        pk_copy = pstrdup(pk_str);
        
        global_index->data[idx] = value_copy;
        global_index->primary_keys[idx] = pk_copy;
    }
    else
    {
        /* Allocate new slot */
        if (global_index->num_records >= global_index->capacity)
        {
            int new_cap = global_index->capacity * 2;
            char **new_data;
            char **new_pks;
            
            //elog(DEBUG1, "biscuit_index_insert: Expanding capacity from %d to %d", global_index->capacity, new_cap);
            
            new_data = (char **)MemoryContextAlloc(index_context,
                                                    new_cap * sizeof(char *));
            new_pks = (char **)MemoryContextAlloc(index_context,
                                                   new_cap * sizeof(char *));

            
            
            memcpy(new_data, global_index->data,
                   global_index->capacity * sizeof(char *));
            memcpy(new_pks, global_index->primary_keys,
                   global_index->capacity * sizeof(char *));
            
            global_index->data = new_data;
            global_index->primary_keys = new_pks;
            global_index->capacity = new_cap;
        }
        
        idx = global_index->num_records;
        //elog(DEBUG1, "biscuit_index_insert: Allocating new slot %u", idx);
        
        value_copy = pstrdup(value);
        pk_copy = pstrdup(pk_str);
        
        global_index->data[idx] = value_copy;
        global_index->primary_keys[idx] = pk_copy;
        global_index->num_records++;
    }
    
    /* CRITICAL FIX: Add to hash table using the STORED pointer */
    biscuit_add_pk_mapping(global_index->primary_keys[idx], idx);
    
    /* Add to indices */
    biscuit_add_to_index(idx, value_copy);
    
    global_index->insert_count++;
    
    //elog(NOTICE, "biscuit_index_insert: Successfully inserted PK='%s' at index %u", pk_str, idx);
    
    MemoryContextSwitchTo(oldcontext);
}

static void biscuit_index_update(const char *pk_str, const char *new_value)
{
    int32_t idx;
    char *old_value;
    char *new_value_copy;
    MemoryContext oldcontext;
    int old_len, new_len;
    
    if (!global_index)
        return;
    
    //elog(DEBUG1, "biscuit_index_update: Called for PK='%s'", pk_str);
    
    /* O(1) hash table lookup */
    idx = biscuit_biscuit_find_index_by_pk_debug(pk_str, false);  /* Enable debug logging */
    if (idx < 0)
    {
        /* If not found, treat as insert */
        //elog(NOTICE, "biscuit_index_update: PK '%s' not found, treating as INSERT", pk_str);
        biscuit_index_insert(pk_str, new_value);
        return;
    }
    
    //elog(NOTICE, "biscuit_index_update: Found existing record at index %d", idx);
    
    oldcontext = MemoryContextSwitchTo(index_context);
    
    old_value = global_index->data[idx];
    new_value_copy = pstrdup(new_value);
    
    old_len = strlen(old_value);
    new_len = strlen(new_value_copy);
    
    /* Try incremental update ONLY for same-length strings */
    if (old_len == new_len && old_len >= 3 && 
        biscuit_update_incremental((uint32_t)idx, old_value, new_value_copy))
    {
        /* Success - just update the data pointer */
        pfree(old_value);
        global_index->data[idx] = new_value_copy;
        global_index->update_count++;
        //elog(NOTICE, "biscuit_index_update: Incremental update successful");
        MemoryContextSwitchTo(oldcontext);
        return;
    }
    
    //elog(NOTICE, "biscuit_index_update: Performing full reindex (old_len=%d, new_len=%d)", old_len, new_len);
    
    /* Full reindex approach */
    biscuit_remove_from_all_indices((uint32_t)idx);
    pfree(old_value);
    global_index->data[idx] = new_value_copy;
    biscuit_add_to_index((uint32_t)idx, new_value_copy);
    
    global_index->update_count++;
    
    MemoryContextSwitchTo(oldcontext);
}

 /* INSERT */
 
/* DELETE (O(1) with lazy tombstones) */
 static void biscuit_index_delete(const char *pk_str)
{
    int32_t idx;
    MemoryContext oldcontext;
    
    if (!global_index || !pk_str)
        return;
    
    /* O(1) hash table lookup */
    idx = biscuit_find_index_by_pk(pk_str);
    if (idx < 0)
        return;
    
    oldcontext = MemoryContextSwitchTo(index_context);
    
    /* CRITICAL FIX: Increment delete_count BEFORE any early returns */
    global_index->delete_count++;
    
    /* Add to tombstone bitmap - O(1) operation! */
    biscuit_roaring_add(global_index->lazy_del.tombstones, (uint32_t)idx);
    
    /* Store the index for cleanup */
    if (global_index->lazy_del.tombstone_count >= global_index->lazy_del.tombstone_capacity)
    {
        uint32_t new_cap = global_index->lazy_del.tombstone_capacity * 2;
        uint32_t *new_indices = (uint32_t *)MemoryContextAlloc(
            index_context,
            new_cap * sizeof(uint32_t)
        );
        memcpy(new_indices, global_index->lazy_del.deleted_indices,
               global_index->lazy_del.tombstone_count * sizeof(uint32_t));
        global_index->lazy_del.deleted_indices = new_indices;
        global_index->lazy_del.tombstone_capacity = new_cap;
    }
    
    global_index->lazy_del.deleted_indices[global_index->lazy_del.tombstone_count] = idx;
    global_index->lazy_del.tombstone_count++;
    
    /* Remove from hash table */
    biscuit_remove_pk_mapping(pk_str);
    
    /* Free PK string */
    if (global_index->primary_keys[idx])
    {
        pfree(global_index->primary_keys[idx]);
        global_index->primary_keys[idx] = NULL;
    }
    
    /* Add to free list for slot reuse */
    biscuit_push_free_index(&global_index->free_list, (uint32_t)idx);
    
    /* Trigger batch cleanup if threshold reached */
    if (global_index->lazy_del.tombstone_count >= 
        global_index->lazy_del.tombstone_threshold)
    {
        biscuit_cleanup_tombstones();
    }
    
    MemoryContextSwitchTo(oldcontext);
}

/* ==================== UPDATE FUNCTION ==================== */

PG_FUNCTION_INFO_V1(biscuit_dump_hash_table);
Datum biscuit_dump_hash_table(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS status;
    PKMapEntry *entry;
    StringInfoData buf;
    int count = 0;
    
    if (!global_index || !global_index->pk_to_index)
    {
        PG_RETURN_TEXT_P(cstring_to_text("No hash table initialized"));
    }
    
    initStringInfo(&buf);
    appendStringInfo(&buf, "Hash Table Contents:\n");
    appendStringInfo(&buf, "====================\n");
    
    hash_seq_init(&status, global_index->pk_to_index);
    
    while ((entry = (PKMapEntry *)hash_seq_search(&status)) != NULL)
    {
        appendStringInfo(&buf, "[%d] PK='%s' -> idx=%u, value='%s'\n",
                        count++,
                        entry->pk_str,
                        entry->idx,
                        global_index->data[entry->idx] ? global_index->data[entry->idx] : "(null)");
    }
    
    appendStringInfo(&buf, "\nTotal entries: %d\n", count);
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

 /* ==================== MAIN QUERY FUNCTION ==================== */
 
 /* In biscuit_query_internal, add this at the start: */

static uint32_t* biscuit_query_internal(const char *pattern, uint64_t *result_count)
{
    RoaringBitmap *result = NULL;
    uint32_t *indices;
    int plen = strlen(pattern);
    int i;
    ParsedPattern *parsed;
    int min_len;
    int underscore_count;
    
    if (global_index) 
        global_index->query_count++;
    
    /* Handle empty pattern - match empty strings only */
    if (plen == 0)
    {
        if (global_index->length_idx.max_length > 0 &&
            global_index->length_idx.length_bitmaps[0])
        {
            result = biscuit_roaring_copy(global_index->length_idx.length_bitmaps[0]);
            
            /* Filter tombstones */
            if (global_index->lazy_del.tombstone_count > 0)
                biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
            
            indices = biscuit_roaring_to_array(result, result_count);
            biscuit_roaring_free(result);
            return indices;
        }
        
        *result_count = 0;
        return NULL;
    }
     
     /* Handle '%' - return all non-deleted records */
     if (plen == 1 && pattern[0] == '%')
     {
         int count = 0;
         indices = (uint32_t *)palloc(global_index->num_records * sizeof(uint32_t));
         for (i = 0; i < global_index->num_records; i++)
         {
             if (global_index->data[i] != NULL)
             {
                 /* Check if not tombstoned */
                 #ifdef HAVE_ROARING
                 if (!roaring_bitmap_contains(global_index->lazy_del.tombstones, (uint32_t)i))
                     indices[count++] = (uint32_t)i;
                 #else
                 uint32_t block = i >> 6;
                 uint32_t bit = i & 63;
                 if (block >= global_index->lazy_del.tombstones->num_blocks ||
                     !(global_index->lazy_del.tombstones->blocks[block] & (1ULL << bit)))
                     indices[count++] = (uint32_t)i;
                 #endif
             }
         }
         *result_count = count;
         return indices;
     }
     
     /* Fast path: wildcard-only patterns */
     if (biscuit_is_wildcard_only(pattern, plen, &underscore_count))
     {
         result = biscuit_match_wildcard_only(pattern, plen, underscore_count);
         indices = biscuit_roaring_to_array(result, result_count);
         biscuit_roaring_free(result);
         return indices;
     }
     
     /* Fast path: simple single-character positioned patterns */
     result = biscuit_match_simple_positioned(pattern, plen);
     if (result)
     {
         indices = biscuit_roaring_to_array(result, result_count);
         biscuit_roaring_free(result);
         return indices;
     }
     
     /* Complex patterns - parse and match */
     parsed = biscuit_parse_pattern(pattern);
     
     if (parsed->part_count == 0)
     {
         biscuit_free_parsed_pattern(parsed);
         int count = 0;
         indices = (uint32_t *)palloc(global_index->num_records * sizeof(uint32_t));
         for (i = 0; i < global_index->num_records; i++)
         {
             if (global_index->data[i] != NULL)
             {
                 /* Check if not tombstoned */
                 #ifdef HAVE_ROARING
                 if (!roaring_bitmap_contains(global_index->lazy_del.tombstones, (uint32_t)i))
                     indices[count++] = (uint32_t)i;
                 #else
                 uint32_t block = i >> 6;
                 uint32_t bit = i & 63;
                 if (block >= global_index->lazy_del.tombstones->num_blocks ||
                     !(global_index->lazy_del.tombstones->blocks[block] & (1ULL << bit)))
                     indices[count++] = (uint32_t)i;
                 #endif
             }
         }
         *result_count = count;
         return indices;
     }
     
     /* Calculate minimum length */
     min_len = 0;
     for (i = 0; i < parsed->part_count; i++)
         min_len += parsed->part_lens[i];
     
     /* Start with length filter */
     if (!parsed->starts_percent && !parsed->ends_percent && parsed->part_count == 1)
     {
         if (min_len < global_index->length_idx.max_length && 
             global_index->length_idx.length_bitmaps[min_len])
         {
             result = biscuit_roaring_copy(global_index->length_idx.length_bitmaps[min_len]);
             
             /* Filter tombstones */
             if (global_index->lazy_del.tombstone_count > 0)
                 biscuit_roaring_andnot_inplace(result, global_index->lazy_del.tombstones);
         }
         else
             result = biscuit_roaring_create();
     }
     else
     {
         result = biscuit_get_length_ge(min_len);
     }
     
     if (biscuit_roaring_is_empty(result))
     {
         biscuit_free_parsed_pattern(parsed);
         indices = biscuit_roaring_to_array(result, result_count);
         biscuit_roaring_free(result);
         return indices;
     }
     
     /* Apply pattern matching based on pattern structure */
     if (parsed->part_count == 1)
     {
         RoaringBitmap *match;
         
         if (!parsed->starts_percent && !parsed->ends_percent)
         {
             match = biscuit_match_part_at_pos(parsed->parts[0], parsed->part_lens[0], 0);
         }
         else if (!parsed->starts_percent && parsed->ends_percent)
         {
             match = biscuit_match_part_at_pos(parsed->parts[0], parsed->part_lens[0], 0);
         }
         else if (parsed->starts_percent && !parsed->ends_percent)
         {
             match = biscuit_match_part_at_end(parsed->parts[0], parsed->part_lens[0]);
         }
         else
         {
             match = biscuit_match_part_anywhere(parsed->parts[0], parsed->part_lens[0]);
         }
         
         biscuit_roaring_and_inplace(result, match);
         biscuit_roaring_free(match);
     }
     else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent)
     {
         RoaringBitmap *prefix_match = biscuit_match_part_at_pos(parsed->parts[0], parsed->part_lens[0], 0);
         RoaringBitmap *suffix_match = biscuit_match_part_at_end(parsed->parts[1], parsed->part_lens[1]);
         
         biscuit_roaring_and_inplace(result, prefix_match);
         biscuit_roaring_and_inplace(result, suffix_match);
         
         biscuit_roaring_free(prefix_match);
         biscuit_roaring_free(suffix_match);
     }
     else if (parsed->part_count >= 2)
     {
         biscuit_roaring_free(result);
         
         result = biscuit_match_multipart_windowed_positions(
             (const char **)parsed->parts,
             parsed->part_lens,
             parsed->part_count,
             parsed->starts_percent,
             parsed->ends_percent
         );
     }
     
     biscuit_free_parsed_pattern(parsed);
     
     indices = biscuit_roaring_to_array(result, result_count);
     biscuit_roaring_free(result);
     return indices;
 }
 
 /* ==================== POSTGRESQL TRIGGER FUNCTION ==================== */
 
 PG_FUNCTION_INFO_V1(biscuit_trigger);
Datum biscuit_trigger(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    HeapTuple tuple;
    Datum pk_datum, col_datum;
    bool pk_isnull, col_isnull;
    char *pk_str;
    char *col_value;
    int pk_attnum, col_attnum;
    Oid pk_type;
    int32_t idx;
    
    if (!CALLED_AS_TRIGGER(fcinfo))
        ereport(ERROR, (errmsg("biscuit_trigger: not called by trigger manager")));
    
    if (!global_index)
    {
        if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
            return PointerGetDatum(trigdata->tg_trigtuple);
        return PointerGetDatum(trigdata->tg_newtuple);
    }
    
    /* Get column numbers */
    pk_attnum = SPI_fnumber(trigdata->tg_relation->rd_att, global_index->pk_column_name);
    col_attnum = SPI_fnumber(trigdata->tg_relation->rd_att, global_index->column_name);
    
    if (pk_attnum == SPI_ERROR_NOATTRIBUTE)
        ereport(ERROR, (errmsg("Column %s not found", global_index->pk_column_name)));
    if (col_attnum == SPI_ERROR_NOATTRIBUTE)
        ereport(ERROR, (errmsg("Column %s not found", global_index->column_name)));
    
    /* Get PK type */
    pk_type = SPI_gettypeid(trigdata->tg_relation->rd_att, pk_attnum);
    
    /* HANDLE INSERT */
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
    {
        tuple = trigdata->tg_trigtuple;
        
        pk_datum = SPI_getbinval(tuple, trigdata->tg_relation->rd_att, pk_attnum, &pk_isnull);
        col_datum = SPI_getbinval(tuple, trigdata->tg_relation->rd_att, col_attnum, &col_isnull);
        
        if (pk_isnull)
            ereport(ERROR, (errmsg("NULL primary key in INSERT")));
        
        /* Convert PK to string */
        pk_str = biscuit_datum_to_pk_string(pk_datum, pk_type, &pk_isnull);
        col_value = col_isnull ? "" : TextDatumGetCString(col_datum);
        
        biscuit_index_insert(pk_str, col_value);
        idx = biscuit_find_index_by_pk(pk_str);
        pfree(pk_str);  /* Free temporary conversion */
        
        return PointerGetDatum(trigdata->tg_trigtuple);
    }
    /* HANDLE UPDATE */
    /* HANDLE UPDATE */
    else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
    {
        tuple = trigdata->tg_newtuple;
        
        pk_datum = SPI_getbinval(tuple, trigdata->tg_relation->rd_att, pk_attnum, &pk_isnull);
        col_datum = SPI_getbinval(tuple, trigdata->tg_relation->rd_att, col_attnum, &col_isnull);
        
        if (pk_isnull)
            ereport(ERROR, (errmsg("NULL primary key in UPDATE")));
        
        /* Convert PK to string */
        pk_str = biscuit_datum_to_pk_string(pk_datum, pk_type, &pk_isnull);
        
        /* ADD THIS DEBUG LINE */
        //elog(NOTICE, "TRIGGER UPDATE: PK='%s', new_value='%s'", pk_str, col_isnull ? "(null)" : TextDatumGetCString(col_datum));
        
        col_value = col_isnull ? "" : TextDatumGetCString(col_datum);
        
        biscuit_index_update(pk_str, col_value);
        idx = biscuit_find_index_by_pk(pk_str);
        
        pfree(pk_str);
        
        return PointerGetDatum(trigdata->tg_newtuple);
    }
    /* HANDLE DELETE */
    else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
    {
        tuple = trigdata->tg_trigtuple;
        
        pk_datum = SPI_getbinval(tuple, trigdata->tg_relation->rd_att, pk_attnum, &pk_isnull);
        
        if (pk_isnull)
            ereport(ERROR, (errmsg("NULL primary key in DELETE")));
        
        /* Convert PK to string */
        pk_str = biscuit_datum_to_pk_string(pk_datum, pk_type, &pk_isnull);
        
        biscuit_index_delete(pk_str);
        
        pfree(pk_str);  /* Free temporary conversion */
        
        return PointerGetDatum(trigdata->tg_trigtuple);
    }
    
    return PointerGetDatum(NULL);
}
 /* ==================== POSTGRESQL QUERY FUNCTIONS ==================== */
 
 PG_FUNCTION_INFO_V1(biscuit_query);
 Datum biscuit_query(PG_FUNCTION_ARGS)
 {
     text *pattern_text = PG_GETARG_TEXT_PP(0);
     char *pattern = text_to_cstring(pattern_text);
     uint64_t result_count = 0;
     uint32_t *results;
     
     if (!global_index)
     {
         PG_RETURN_INT32(0);
     }
     
     results = biscuit_query_internal(pattern, &result_count);
     
     if (results)
         pfree(results);
     
     PG_RETURN_INT32((int32_t)result_count);
 }


PG_FUNCTION_INFO_V1(biscuit_query_rows);
Datum biscuit_query_rows(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        text *pattern_text = PG_GETARG_TEXT_PP(0);
        char *pattern = text_to_cstring(pattern_text);
        uint64_t result_count = 0;
        uint32_t *matches;
        TupleDesc tupdesc;
        AttInMetadata *attinmeta;
        
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        if (!global_index)
        {
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }
        
        matches = biscuit_query_internal(pattern, &result_count);
        funcctx->max_calls = result_count;
        funcctx->user_fctx = (void *)matches;
        
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errmsg("function returning record in invalid context")));
        
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        funcctx->tuple_desc = tupdesc;
        
        MemoryContextSwitchTo(oldcontext);
    }
    
    funcctx = SRF_PERCALL_SETUP();
    
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        uint32_t *matches = (uint32_t *)funcctx->user_fctx;
        uint32_t array_idx = matches[funcctx->call_cntr];
        char *values[2];
        HeapTuple tuple;
        Datum result;
        
        /* OPTIMIZATION: Use direct pointers, avoid CStringGetTextDatum */
        values[0] = global_index->primary_keys[array_idx];
        values[1] = global_index->data[array_idx];
        
        tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
        result = HeapTupleGetDatum(tuple);
        
        SRF_RETURN_NEXT(funcctx, result);
    }
    
    if (funcctx->user_fctx)
    {
        pfree(funcctx->user_fctx);
        funcctx->user_fctx = NULL;
    }
    
    SRF_RETURN_DONE(funcctx);
}
 
 /* ==================== INDEX BUILD FUNCTION ==================== */
 
 PG_FUNCTION_INFO_V1(build_biscuit_index);
Datum build_biscuit_index(PG_FUNCTION_ARGS)
{
    text *table_name;
    text *column_name;
    text *pk_column_name;
    char *table_str;
    char *column_str;
    char *pk_column_str;
    
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errmsg("table_name cannot be NULL")));
    if (PG_ARGISNULL(1))
        ereport(ERROR, (errmsg("column_name cannot be NULL")));
    
    table_name = PG_GETARG_TEXT_PP(0);
    column_name = PG_GETARG_TEXT_PP(1);
    table_str = text_to_cstring(table_name);
    column_str = text_to_cstring(column_name);
    
    if (PG_NARGS() < 3 || PG_ARGISNULL(2))
    {
        pk_column_str = pstrdup("id");
    }
    else
    {
        pk_column_name = PG_GETARG_TEXT_PP(2);
        pk_column_str = text_to_cstring(pk_column_name);
    }
    
    instr_time start_time, end_time;
    StringInfoData query;
    int ret, num_records, idx, len, pos;
    MemoryContext oldcontext;
    HeapTuple tuple;
    bool isnull;
    Datum datum;
    text *txt;
    char *str;
    unsigned char uch;
    RoaringBitmap *existing_bm;
    int ch_idx;
    double ms;
    int i;
    int neg_offset;
    Oid pk_type;
    
    INSTR_TIME_SET_CURRENT(start_time);
    
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("SPI_connect failed")));
    
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT %s, %s FROM %s ORDER BY %s",
                     quote_identifier(pk_column_str),
                     quote_identifier(column_str),
                     quote_identifier(table_str),
                     quote_identifier(pk_column_str));
    
    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        SPI_finish();
        ereport(ERROR, (errmsg("Query failed")));
    }
    
    num_records = SPI_processed;
    
    if (index_context)
        MemoryContextDelete(index_context);
    
    index_context = AllocSetContextCreate(TopMemoryContext,
                                         "RoaringLikeIndex",
                                         ALLOCSET_DEFAULT_SIZES);
    
    oldcontext = MemoryContextSwitchTo(index_context);
    
    global_index = (RoaringIndex *)MemoryContextAlloc(index_context, sizeof(RoaringIndex));
    global_index->num_records = num_records;
    global_index->capacity = num_records;
    global_index->max_len = 0;
    global_index->memory_used = 0;
    global_index->data = (char **)MemoryContextAlloc(index_context, num_records * sizeof(char *));
    global_index->primary_keys = (char **)MemoryContextAlloc(index_context, num_records * sizeof(char *));
    

    global_index->table_name = MemoryContextStrdup(index_context, table_str);
    global_index->column_name = MemoryContextStrdup(index_context, column_str);
    global_index->pk_column_name = MemoryContextStrdup(index_context, pk_column_str);
    
    /* Initialize statistics */
    global_index->insert_count = 0;
    global_index->update_count = 0;
    global_index->delete_count = 0;
    global_index->incremental_update_count = 0;
    global_index->query_count = 0;
    
    /* Initialize free list */
    biscuit_init_free_list(&global_index->free_list);
    
    /* Initialize hash table for O(1) PK lookup */
    biscuit_init_pk_hash_table();
    
    /* Initialize lazy deletion */
    biscuit_init_lazy_deletion();
    
    for (ch_idx = 0; ch_idx < CHAR_RANGE; ch_idx++)
    {
        global_index->pos_idx[ch_idx].entries = (PosEntry *)MemoryContextAlloc(index_context, INITIAL_CAPACITY * sizeof(PosEntry));
        global_index->pos_idx[ch_idx].count = 0;
        global_index->pos_idx[ch_idx].capacity = INITIAL_CAPACITY;
        
        global_index->neg_idx[ch_idx].entries = (PosEntry *)MemoryContextAlloc(index_context, INITIAL_CAPACITY * sizeof(PosEntry));
        global_index->neg_idx[ch_idx].count = 0;
        global_index->neg_idx[ch_idx].capacity = INITIAL_CAPACITY;
        
        global_index->char_cache[ch_idx] = NULL;
    }
    
    global_index->length_idx.max_length = 0;
    global_index->length_idx.length_bitmaps = NULL;
    
    pk_type = SPI_gettypeid(SPI_tuptable->tupdesc, 1);

    /* ==================== CRITICAL FIX: PK HANDLING ==================== */
    for (idx = 0; idx < num_records; idx++)
    {
        char *pk_str;
        
        tuple = SPI_tuptable->vals[idx];
        
        datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
        if (isnull)
        {
            ereport(ERROR, (errmsg("NULL primary key found at record %d", idx)));
        }
        
        /* Convert PK to string (works for ANY type) */
        pk_str = biscuit_datum_to_pk_string(datum, pk_type, &isnull);
        
        /* CRITICAL FIX: Store PK persistently FIRST */
        global_index->primary_keys[idx] = MemoryContextStrdup(index_context, pk_str);
        
        /* Then add to hash table using the STORED pointer */
        biscuit_add_pk_mapping(global_index->primary_keys[idx], (uint32_t)idx);
        
        pfree(pk_str);  /* Free temporary conversion */
        
        /* Now index the value column */
        datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
        
        if (isnull)
        {
            global_index->data[idx] = MemoryContextStrdup(index_context, "");
            continue;
        }
        
        txt = DatumGetTextPP(datum);
        str = text_to_cstring(txt);
        len = strlen(str);
        
        if (len > MAX_POSITIONS)
            len = MAX_POSITIONS;
        
        global_index->data[idx] = MemoryContextStrdup(index_context, str);
        if (len > global_index->max_len)
            global_index->max_len = len;
        
        for (pos = 0; pos < len; pos++)
        {
            uch = (unsigned char)str[pos];
            
            existing_bm = biscuit_get_pos_bitmap(uch, pos);
            if (!existing_bm)
            {
                existing_bm = biscuit_roaring_create();
                biscuit_set_pos_bitmap(uch, pos, existing_bm);
            }
            biscuit_roaring_add(existing_bm, (uint32_t)idx);
            
            neg_offset = -(len - pos);
            
            existing_bm = biscuit_get_neg_bitmap(uch, neg_offset);
            if (!existing_bm)
            {
                existing_bm = biscuit_roaring_create();
                biscuit_set_neg_bitmap(uch, neg_offset, existing_bm);
            }
            biscuit_roaring_add(existing_bm, (uint32_t)idx);
        }
        
        pfree(str);
    }
    
    for (ch_idx = 0; ch_idx < CHAR_RANGE; ch_idx++)
    {
        CharIndex *cidx = &global_index->pos_idx[ch_idx];
        
        if (cidx->count == 0)
            continue;
        
        RoaringBitmap *new_bm = biscuit_roaring_copy(cidx->entries[0].bitmap);
        
        for (i = 1; i < cidx->count; i++)
        {
            biscuit_roaring_or_inplace(new_bm, cidx->entries[i].bitmap);
        }
        
        global_index->char_cache[ch_idx] = new_bm;
    }
    
    global_index->length_idx.max_length = global_index->max_len + 1;
    global_index->length_idx.length_bitmaps = (RoaringBitmap **)MemoryContextAlloc(
        index_context, 
        global_index->length_idx.max_length * sizeof(RoaringBitmap *)
    );
    
    for (i = 0; i < global_index->length_idx.max_length; i++)
        global_index->length_idx.length_bitmaps[i] = NULL;
    
    for (idx = 0; idx < num_records; idx++)
    {
        len = strlen(global_index->data[idx]);
        if (len >= global_index->length_idx.max_length)
            continue;
        
        if (!global_index->length_idx.length_bitmaps[len])
            global_index->length_idx.length_bitmaps[len] = biscuit_roaring_create();
        
        biscuit_roaring_add(global_index->length_idx.length_bitmaps[len], (uint32_t)idx);
    }
    
    global_index->memory_used = sizeof(RoaringIndex);
    for (ch_idx = 0; ch_idx < CHAR_RANGE; ch_idx++)
    {
        if (global_index->char_cache[ch_idx])
            global_index->memory_used += biscuit_roaring_size_bytes(global_index->char_cache[ch_idx]);
        
        global_index->memory_used += global_index->pos_idx[ch_idx].count * sizeof(PosEntry);
        global_index->memory_used += global_index->neg_idx[ch_idx].count * sizeof(PosEntry);
    }
    for (i = 0; i < global_index->length_idx.max_length; i++)
    {
        if (global_index->length_idx.length_bitmaps[i])
            global_index->memory_used += biscuit_roaring_size_bytes(global_index->length_idx.length_bitmaps[i]);
    }
    
    MemoryContextSwitchTo(oldcontext);
    SPI_finish();
    
    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    ms = INSTR_TIME_GET_MILLISEC(end_time);
    
    PG_RETURN_BOOL(true);
}
 /* ==================== STATUS AND CLEANUP FUNCTIONS ==================== */
 
 PG_FUNCTION_INFO_V1(biscuit_status);
Datum biscuit_status(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    int active_records = 0;
    int i;
    double delete_ratio, query_ratio;
    bool is_tombstoned;
    
    if (!global_index)
    {
        PG_RETURN_TEXT_P(cstring_to_text("No index loaded. Call build_biscuit_index() first."));
    }
    
    /* FIXED: Count active records excluding tombstones */
    for (i = 0; i < global_index->num_records; i++)
    {
        if (global_index->data[i] != NULL)
        {
            /* Check if tombstoned */
            is_tombstoned = false;
            #ifdef HAVE_ROARING
            is_tombstoned = roaring_bitmap_contains(global_index->lazy_del.tombstones, (uint32_t)i);
            #else
            uint32_t block = i >> 6;
            uint32_t bit = i & 63;
            is_tombstoned = (block < global_index->lazy_del.tombstones->num_blocks &&
                            (global_index->lazy_del.tombstones->blocks[block] & (1ULL << bit)));
            #endif
            
            if (!is_tombstoned)
                active_records++;
        }
    }
    
    /* Calculate ratios */
    delete_ratio = (global_index->delete_count > 0) ?
        (100.0 * global_index->lazy_del.tombstone_count) / global_index->delete_count : 0.0;
    query_ratio = (global_index->delete_count > 0) ?
        (double)global_index->query_count / global_index->delete_count : 0.0;
    
    initStringInfo(&buf);
    appendStringInfo(&buf, "========================================\n");
    appendStringInfo(&buf, "Biscuit Index v3 - FIXED\n");
    appendStringInfo(&buf, "========================================\n");
    appendStringInfo(&buf, "Table: %s\n", global_index->table_name);
    appendStringInfo(&buf, "Column: %s\n", global_index->column_name);
    appendStringInfo(&buf, "Primary Key: %s\n", global_index->pk_column_name);
    appendStringInfo(&buf, "Active Records: %d\n", active_records);
    appendStringInfo(&buf, "Total Slots: %d\n", global_index->num_records);
    appendStringInfo(&buf, "Free Slots: %d\n", global_index->free_list.count);
    appendStringInfo(&buf, "Tombstoned Slots: %d\n", global_index->lazy_del.tombstone_count);
    appendStringInfo(&buf, "Max length: %d\n", global_index->max_len);
    appendStringInfo(&buf, "Memory: %zu bytes (%.2f MB)\n", 
                    global_index->memory_used,
                    global_index->memory_used / (1024.0 * 1024.0));
    appendStringInfo(&buf, "----------------------------------------\n");
    appendStringInfo(&buf, "CRUD Statistics:\n");
    appendStringInfo(&buf, "  Inserts: %lld\n", (long long)global_index->insert_count);
    appendStringInfo(&buf, "  Deletes: %lld\n", (long long)global_index->delete_count);
    appendStringInfo(&buf, "  Updates: %lld (incr: %lld, %.1f%%)\n", 
                    (long long)global_index->update_count,
                    (long long)global_index->incremental_update_count,
                    global_index->update_count > 0 ?
                        (100.0 * global_index->incremental_update_count) / global_index->update_count : 0.0);
    appendStringInfo(&buf, "  Queries: %lld\n", (long long)global_index->query_count);
    appendStringInfo(&buf, "----------------------------------------\n");
    appendStringInfo(&buf, "Lazy Deletion Status:\n");
    appendStringInfo(&buf, "  Pending tombstones: %d (%.1f%% of deletes)\n",
                    global_index->lazy_del.tombstone_count, delete_ratio);
    appendStringInfo(&buf, "  Cleanup threshold: %d\n", 
                    global_index->lazy_del.tombstone_threshold);
    appendStringInfo(&buf, "  Total cleanups: %lld\n",
                    (long long)global_index->lazy_del.total_cleanups);
    appendStringInfo(&buf, "  Items cleaned: %lld\n",
                    (long long)global_index->lazy_del.items_cleaned);
    appendStringInfo(&buf, "  Query/Delete ratio: %.2f\n", query_ratio);
    appendStringInfo(&buf, "----------------------------------------\n");
    appendStringInfo(&buf, "Optimizations:\n");
    appendStringInfo(&buf, "  ✓ O(1) hash table PK lookup\n");
    appendStringInfo(&buf, "  ✓ TRUE lazy deletion (O(1) delete!)\n");
    appendStringInfo(&buf, "  ✓ Smart batch cleanup\n");
    appendStringInfo(&buf, "  ✓ Incremental updates\n");
    appendStringInfo(&buf, "  ✓ Tombstone filtering at source\n");
    appendStringInfo(&buf, "  ✓ FIXED: Slot resurrection on reuse\n");
    appendStringInfo(&buf, "========================================\n");
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
 /* Manual cleanup function */
 PG_FUNCTION_INFO_V1(biscuit_cleanup);
 Datum biscuit_cleanup(PG_FUNCTION_ARGS)
 {
     StringInfoData buf;
     int before_count, after_count;
     
     if (!global_index)
     {
         PG_RETURN_TEXT_P(cstring_to_text("No index loaded"));
     }
     
     before_count = global_index->lazy_del.tombstone_count;
     
     if (before_count == 0)
     {
         PG_RETURN_TEXT_P(cstring_to_text("No tombstones to clean"));
     }
     
     biscuit_cleanup_tombstones();
     
     after_count = global_index->lazy_del.tombstone_count;
     
     initStringInfo(&buf);
     appendStringInfo(&buf, "Tombstone cleanup complete:\n");
     appendStringInfo(&buf, "  Cleaned: %d tombstones\n", before_count);
     appendStringInfo(&buf, "  Remaining: %d\n", after_count);
     appendStringInfo(&buf, "  Total cleanups: %lld\n",
                     (long long)global_index->lazy_del.total_cleanups);
     
     PG_RETURN_TEXT_P(cstring_to_text(buf.data));
 }
 
