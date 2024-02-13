import os,sys;

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
      ,target_tablespace_name
      ,result_count = None
      ,result_gb    = None
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
      
      curs = self._parent._sqliteconn.cursor();      
      
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
            rez.append('ALTER TABLE ' + owner + '.' + segment_name + ' MOVE TABLESPACE ' + target_tablespace_name + ';');
            rez = rez + Table(self._parent,owner,segment_name).rebuild_indexes();
            
         else:
            rez.append('/* ' + str(segment_type) + ': ' + str(owner) + '.' + str(segment_name) + ' ' + str(partition_name) + ' */');
         
         running_gb += bytes / 1024 / 1024 / 1024;
         
         if result_gb is not None \
         and running_gb > result_gb:
            exit;
            
      curs.close();
      
      return rez;
      