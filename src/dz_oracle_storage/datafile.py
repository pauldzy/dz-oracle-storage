import os,sys,math;
from .table import Table;
from .index import Index;
from .lob   import Lob;

############################################################################### 
class Datafile(object):

   def __init__(
       self
      ,parent
      ,file_name      : str
      ,file_id        : int
      ,tablespace_name: str
      ,blocks         : int
      ,bytes_allocated: float
      ,bytes_used     : float
      ,bytes_free     : float
      ,max_free_bytes : float
      ,extents_hmw    : int   = None
   ):
   
      self._parent             = parent;
      self._sqliteconn         = parent._sqliteconn;
      self._file_name: str     = file_name;
      self._file_id            = file_id;
      self._tablespace_name    = tablespace_name;
      self._blocks             = blocks;
      self._bytes_allocated    = bytes_allocated;
      self._bytes_used         = bytes_used;
      self._bytes_free         = bytes_free;
      self._max_free_bytes     = max_free_bytes;
      self._extents_hmw        = extents_hmw;
      
   @property
   def file_name(self):
      return self._file_name;
      
   @property
   def file_id(self):
      return self._file_id;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
      
   @property
   def blocks(self):
      return self._blocks;
      
   ####
   def bytes_allocated(
      self
   ) -> float:
      return self._bytes_allocated;
      
   ####
   def gb_allocated(
      self
   ) -> float:
      return self.bytes_allocated() / 1024 / 1024 / 1024;
 
   ####
   def bytes_used(
      self
   ) -> float:
      return self._bytes_used;
      
   ####
   def gb_used(
      self
   ) -> float:
      return self.bytes_used() / 1024 / 1024 / 1024;

   ####
   def bytes_free(
      self
   ) -> float:
      return self._bytes_free;
   
   ####
   def gb_free(
      self
   ) -> float:
      return self.bytes_free() / 1024 / 1024 / 1024;
      
   ####
   def bytes_resizeable(
      self
   ) -> float:
      
      if self._extents_hmw is None:
         highwatermark = 1;
      else:
         highwatermark = self._extents_hmw;

      current_size_bytes = math.ceil(self._blocks  * self._parent.db_block_size);
      target_size_bytes  = math.ceil(highwatermark * self._parent.db_block_size);
      
      return current_size_bytes - target_size_bytes;
   
   ####
   def gb_resizeable(
      self
   ) -> float:
      return self.bytes_resizeable() / 1024 / 1024 / 1024;
      
   ####
   def move_from_hw(
       self
      ,target_tablespace_name: str
      ,result_count          : int   = None
      ,result_gb             : float = None
      ,rebuild_spatial       : bool  = True
   ) -> list[str]:
   
      if not self._parent.harvest_extents:
         raise Exception("parent instance was not harvested with extent information.");
         
      if self.tablespace_name in [
         'SYSTEM','SYSAUX','MDSYS','CTXSYS','ORDS_METADATA'
      ]:
         raise Exception("reorganizing to change hw is not a good idea against system tablespaces.");
         
      if result_count is None and result_gb is None:
         result_count = 25;
         
      elif result_count is None and result_gb is not None:
         result_count = 1000;
      
      curs  = self._parent._sqliteconn.cursor();
      curs2 = self._parent._sqliteconn.cursor();      
      
      str_sql = """
         SELECT
          a.owner
         ,a.segment_name
         ,a.partition_name
         ,a.segment_type
         ,a.block_id
         ,a.end_block
         ,a.blocks
         ,a.bytes
         FROM (
            SELECT
             aa.owner
            ,aa.segment_name
            ,aa.partition_name
            ,aa.segment_type
            ,aa.block_id
            ,aa.block_id + aa.blocks - 1 AS end_block
            ,aa.blocks
            ,aa.bytes
            FROM
            dba_extents aa
            WHERE
            aa.file_id = :p01
            UNION ALL
            SELECT
             'free' AS owner
            ,'free' AS segment_name
            ,NULL   AS partition_name
            ,NULL   AS segment_type
            ,bb.block_id
            ,bb.block_id + bb.blocks - 1 AS end_block
            ,bb.blocks
            ,bb.bytes
            FROM
            dba_free_space bb
            WHERE
            bb.file_id = :p02
            ORDER BY 5
         ) a         
         LIMIT :p03
      """;
 
      curs.execute(
          str_sql
         ,{
             'p01': self.file_id
            ,'p02': self.file_id
            ,'p03': result_count
          }
      );
      
      already_done = {};
      running_gb = 0;
      rez = [];
      for row in curs:
         owner          = row[0];
         segment_name   = row[1];
         partition_name = row[2];
         segment_type   = row[3];
         block_id       = row[4];
         end_block      = row[5];
         blocks         = row[6];
         bytes          = row[7];

         key = owner + segment_name + str(partition_name);
         if owner == 'free' or key not in already_done:
         
            if segment_type == 'TABLE':
               str_sql2 = """
                  SELECT
                   a.owner
                  ,a.segment_name AS table_name
                  ,a.partitioned
                  ,a.tablespace_name
                  ,a.compression
                  ,a.iot_type
                  ,a.temporary
                  ,a.secondary
                  FROM
                  segments_compression a
                  WHERE
                      a.owner        = :p01
                  AND a.segment_name = :p02
                  AND a.segment_type = 'TABLE'
               """;
               
               curs2.execute(
                   str_sql2
                  ,{
                      'p01': owner
                     ,'p02': segment_name
                   }
               );
               
               table_owner           = None;
               table_name            = None;
               table_partitioned     = None;
               table_tablespace_name = None;
               table_compression     = None;
               table_iot_type        = None;
               table_temporary       = None;
               table_secondary       = None;
               for row in curs2:
                  table_owner           = row[0];
                  table_name            = row[1];
                  table_partitioned     = row[2];
                  table_tablespace_name = row[3];
                  table_compression     = row[4];
                  table_iot_type        = row[5];
                  table_temporary       = row[6];
                  table_secondary       = row[7];
               
               if table_secondary == 'N':
                  tbl = Table(
                      parent          = self._parent
                     ,table_owner     = table_owner
                     ,table_name      = table_name
                     ,tablespace_name = table_tablespace_name
                     ,compression     = table_compression
                  );
                  rez = rez + tbl.rebuild(
                     move_tablespace = target_tablespace_name
                  );
                  rez = rez + tbl.rebuild_indexes(
                     rebuild_spatial = rebuild_spatial
                  );
                  
               else:
                  str_sql2 = """
                     SELECT
                      a.sdo_index_owner
                     ,a.sdo_index_name
                     ,b.index_type
                     ,b.table_owner
                     ,b.table_name
                     ,b.parameters
                     ,b.ityp_owner
                     ,b.ityp_name
                     ,b.index_columns
                     FROM
                     sdo_index_metadata_table a
                     JOIN
                     dba_indexes_plus b
                     ON
                         a.sdo_index_owner = b.owner
                     AND a.sdo_index_name  = b.index_name
                     WHERE
                         a.sdo_index_owner = :p01
                     AND a.sdo_index_table = :p02
                  """;
                  
                  curs2.execute(
                      str_sql2
                     ,{
                         'p01': owner
                        ,'p02': segment_name
                      }
                  );
                  
                  sdo_index_owner  = None;
                  sdo_index_name   = None;
                  index_type       = None;
                  table_owner      = None;
                  table_name       = None;
                  index_parameters = None;
                  ityp_owner       = None;
                  ityp_name        = None;
                  index_columns    = None;
                  for row in curs2:
                     sdo_index_owner  = row[0];
                     sdo_index_name   = row[1];
                     index_type       = row[2];
                     table_owner      = row[3];
                     table_name       = row[4];
                     index_parameters = row[5];
                     ityp_owner       = row[6];
                     ityp_name        = row[7];
                     index_columns    = row[8];
                     
                  if sdo_index_name is not None:
                     
                     rez = rez + Index(
                         self
                        ,index_owner      = sdo_index_owner
                        ,index_name       = sdo_index_name
                        ,index_type       = index_type
                        ,table_owner      = table_owner
                        ,table_name       = table_name
                        ,index_parameters = index_parameters
                        ,ityp_owner       = ityp_owner
                        ,ityp_name        = ityp_name
                        ,index_columns    = index_columns
                     ).rebuild(
                         rebuild_spatial = rebuild_spatial
                        ,move_tablespace = target_tablespace_name 
                     );
                     
                  else:               
                     rez.append('/* Secondary table */');
            
            elif segment_type in ['LOBSEGMENT','LOBINDEX']:
               str_sql2 = """
                  SELECT
                   a.owner
                  ,a.table_name
                  ,a.column_name
                  ,a.segment_name
                  ,a.tablespace_name
                  ,a.index_name
                  ,a.securefile
                  ,b.compression
                  ,b.src_compression
                  ,a.varray_type_owner
                  ,a.varray_type_name
                  FROM (
                     SELECT
                      aa.owner
                     ,aa.table_name
                     ,aa.column_name
                     ,aa.segment_name
                     ,aa.tablespace_name
                     ,aa.index_name
                     ,aa.securefile
                     ,bb.type_owner AS varray_type_owner
                     ,bb.type_name  AS varray_type_name
                     FROM
                     dba_lobs aa
                     LEFT JOIN
                     dba_varrays bb
                     ON
                         aa.owner        = bb.owner
                     AND aa.segment_name = bb.lob_name
                     WHERE
                         aa.owner        = :p01
                     AND aa.segment_name = :p02
                     
                     UNION ALL
                     
                     SELECT
                      cc.owner
                     ,cc.table_name
                     ,cc.column_name
                     ,cc.segment_name
                     ,cc.tablespace_name
                     ,cc.index_name
                     ,cc.securefile
                     ,dd.type_owner AS varray_type_owner
                     ,dd.type_name  AS varray_type_name
                     FROM
                     dba_lobs cc
                     LEFT JOIN
                     dba_varrays dd
                     ON
                         cc.owner        = dd.owner
                     AND cc.segment_name = dd.lob_name
                     WHERE
                         cc.owner        = :p03
                     AND cc.index_name   = :p04
                  ) a
                  LEFT JOIN
                  segments_compression b
                  ON
                      a.owner        = b.owner
                  AND a.segment_name = b.segment_name
               """;
               
               curs2.execute(
                   str_sql2
                  ,{
                      'p01': owner
                     ,'p02': segment_name
                     ,'p03': owner
                     ,'p04': segment_name
                   }
               );
               
               lob_owner           = None;
               lob_table_name      = None;
               lob_column_name     = None;
               lob_segment_name    = None;
               lob_tablespace_name = None;
               lob_index_name      = None;
               lob_securefile      = None;
               lob_compression     = None;
               lob_src_compression = None;
               varray_type_owner   = None;
               varray_type_name    = None;
               for row in curs2:
                  lob_owner           = row[0];
                  lob_table_name      = row[1];
                  lob_column_name     = row[2];
                  lob_segment_name    = row[3];
                  lob_tablespace_name = row[4];
                  lob_index_name      = row[5];
                  lob_securefile      = row[6];
                  lob_compression     = row[7];
                  lob_src_compression = row[8];
                  varray_type_owner   = row[9];
                  varray_type_name    = row[10];

               if lob_owner is not None:
                  rez = rez + Lob(
                      parent            = self._parent
                     ,owner             = lob_owner
                     ,table_name        = lob_table_name
                     ,column_name       = lob_column_name
                     ,segment_name      = lob_segment_name
                     ,tablespace_name   = lob_tablespace_name
                     ,index_name        = lob_index_name
                     ,securefile        = lob_securefile
                     ,compression       = lob_compression
                     ,src_compression   = lob_src_compression
                     ,varray_type_owner = varray_type_owner
                     ,varray_type_name  = varray_type_name
                  ).rebuild();
            
            elif segment_type == 'INDEX':
               rez = rez + Index(self._parent,owner,segment_name).rebuild(
                   rebuild_spatial = rebuild_spatial
                  ,move_tablespace = target_tablespace_name
               );
               
            else:
               rez.append('/* ' + str(segment_type) + ': ' + str(owner) + '.' + str(segment_name) + ' ' + str(partition_name) + ' */');
            
            running_gb += bytes / 1024 / 1024 / 1024;
            
            if result_gb is not None \
            and running_gb > result_gb:
               exit;
            
            already_done[key] = 0;
            
      curs.close();
      curs2.close();
      
      return rez;
      