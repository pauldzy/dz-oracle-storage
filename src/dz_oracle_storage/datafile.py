import os,sys;
from .util import spatial_parms;
from .table import Table;

############################################################################### 
class Datafile(object):

   def __init__(
       self
      ,parent
      ,file_name
      ,file_id
      ,tablespace_name
      ,bytes_allocated
      ,bytes_used
      ,bytes_free
      ,max_free_bytes
      ,extents_hmw   = None
      ,db_block_size = None
   ):
   
      self._parent             = parent;
      self._file_name          = file_name;
      self._file_id            = file_id;
      self._tablespace_name    = tablespace_name;
      self._bytes_allocated    = bytes_allocated;
      self._bytes_used         = bytes_used;
      self._bytes_free         = bytes_free;
      self._max_free_bytes     = max_free_bytes;
      self._extents_hmw        = extents_hmw;
      self._db_block_size      = db_block_size;
      
   @property
   def file_name(self):
      return self._file_name;
      
   @property
   def file_id(self):
      return self._file_id;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
      
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
   def move_from_hw(
       self
      ,target_tablespace_name: str
      ,result_count: int     = None
      ,result_gb: float      = None
      ,rebuild_spatial: bool = True
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
         
         if segment_type == 'TABLE':
            str_sql2 = """
               SELECT
                a.owner
               ,a.table_name
               ,a.partitioned
               ,a.iot_type
               ,a.temporary
               ,a.secondary
               FROM
               dba_tables a
               WHERE
                   a.owner      = :p01
               AND a.table_name = :p02
            """;
            
            curs2.execute(
                str_sql2
               ,{
                   'p01': owner
                  ,'p02': segment_name
                }
            );
            
            table_owner       = None;
            table_name        = None;
            table_partitioned = None;
            table_iot_type    = None;
            table_temporary   = None;
            table_secondary   = None;
            for row in curs2:
               table_owner       = row[0];
               table_name        = row[1];
               table_partitioned = row[2];
               table_iot_type    = row[3];
               table_temporary   = row[4];
               table_secondary   = row[5];
            
            if table_secondary == 'N':
               rez.append('ALTER TABLE ' + owner + '.' + segment_name + ' MOVE TABLESPACE ' + target_tablespace_name + ';');
               rez = rez + Table(self._parent,owner,segment_name).rebuild_indexes(rebuild_spatial=rebuild_spatial);
               
            else:
               str_sql2 = """
                  SELECT
                   a.sdo_index_owner
                  ,a.sdo_index_name
                  ,b.table_owner
                  ,b.table_name
                  ,b.index_columns
                  ,b.parameters
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
               table_owner      = None;
               table_name       = None;
               index_columns    = None;
               index_parameters = None;
               for row in curs2:
                  sdo_index_owner  = row[0];
                  sdo_index_name   = row[1];
                  table_owner      = row[2];
                  table_name       = row[3];
                  index_columns    = row[4];
                  index_parameters = row[5];
                  
               if sdo_index_name is not None:
                  
                  if rebuild_spatial:
                     prms = spatial_parms(
                         parms        = index_parameters
                        ,inject_parms = {'TABLESPACE':target_tablespace_name}
                     );
                     
                     rez.append('DROP INDEX ' + sdo_index_owner + '.' + sdo_index_name + ';');
                     rez.append('CREATE INDEX ' + sdo_index_owner + '.' + sdo_index_name + ' ' \
                     + 'ON ' + table_owner + '.' + table_name       \
                     + '(' + index_columns + ') '                                    \
                     + 'INDEXTYPE IS "MDSYS"."SPATIAL_INDEX_V2" '  + prms + ';'); 
                     
                  else:
                     rez.append('ALTER INDEX ' + sdo_index_owner + '.' + sdo_index_name + ' ' \
                        + 'REBUILD TABLESPACE ' + target_tablespace_name + ';');
               
               else:               
                  rez.append('/* Secondary table */');
         
         elif segment_type == 'LOBSEGMENT':
            str_sql2 = """
               SELECT
                a.owner
               ,a.table_name
               ,a.column_name
               ,a.securefile
               ,b.type_owner
               ,b.type_name
               FROM
               dba_lobs a
               LEFT JOIN
               dba_varrays b
               ON
                   a.owner        = b.owner
               AND a.segment_name = b.lob_name
               WHERE
                   a.owner        = :p01
               AND a.segment_name = :p02
            """;
            
            curs2.execute(
                str_sql2
               ,{
                   'p01': owner
                  ,'p02': segment_name
                }
            );
            
            lob_owner         = None;
            lob_table_name    = None;
            lob_column_name   = None;
            lob_securefile    = None;
            varray_type_owner = None;
            varray_type_name  = None;
            for row in curs2:
               lob_owner         = row[0];
               lob_table_name    = row[1];
               lob_column_name   = row[2];
               lob_securefile    = row[3];
               varray_type_owner = row[4];
               varray_type_name  = row[5];

            if lob_owner is not None:
               if varray_type_owner is not None:
                  if lob_securefile == 'YES':
                     rez.append('ALTER TABLE ' + lob_owner + '.' + lob_table_name + ' '         \
                        + 'MOVE VARRAY ' + lob_column_name + ' '                                \
                        + 'STORE AS SECUREFILE LOB(TABLESPACE ' + target_tablespace_name + ');');
                        
                  else:
                     rez.append('ALTER TABLE ' + lob_owner + '.' + lob_table_name + ' '      \
                        + 'MOVE VARRAY ' + lob_column_name + ' '                             \
                        + 'STORE AS LOB(TABLESPACE ' + target_tablespace_name + ');');
               
               else:
                  rez.append('ALTER TABLE ' + lob_owner + '.' + lob_table_name + ' '   \
                     + 'MOVE LOB(' + lob_column_name + ') '                            \
                     + 'STORE AS (TABLESPACE ' + target_tablespace_name + ');');
                     
               rez = rez + Table(self._parent,lob_owner,lob_table_name).rebuild_indexes();
         
         elif segment_type == 'INDEX':
            rez.append('ALTER INDEX ' + owner + '.' + segment_name + ' ' \
                     + 'REBUILD TABLESPACE ' + target_tablespace_name + ';');
         
         else:
            rez.append('/* ' + str(segment_type) + ': ' + str(owner) + '.' + str(segment_name) + ' ' + str(partition_name) + ' */');
         
         running_gb += bytes / 1024 / 1024 / 1024;
         
         if result_gb is not None \
         and running_gb > result_gb:
            exit;
            
      curs.close();
      curs2.close();
      
      return rez;
      