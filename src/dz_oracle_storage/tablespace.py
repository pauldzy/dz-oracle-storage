import os,sys;
from .util  import dzq;
from .ddl   import DDL;

############################################################################### 
class Tablespace(object):

   def __init__(
       self
      ,parent
      ,tablespace_name: str
      ,bytes_allocated: float
      ,bytes_used     : float
      ,bytes_free     : float
   ):
   
      self._parent           = parent;
      self._sqliteconn       = parent._sqliteconn;
      self._tablespace_name  = tablespace_name;
      self._bytes_allocated  = bytes_allocated;
      self._bytes_used       = bytes_used;
      self._datafiles        = {};
      
      # bytes free may or may not include recyclebin
      self._bytes_free       = bytes_free;
      self._bytes_recyclebin = None;
      
      self._bytes_comp_none  = None;
      self._bytes_comp_low   = None;
      self._bytes_comp_high  = None;
      self._bytes_comp_unk   = None;
      
   @property
   def tablespace_name(self):
      return dzq(self._tablespace_name);
 
   @property
   def datafiles(self): 
      return self._datafiles;
         
   @property
   def datafiles_l(self):
      return [d for d in self.datafiles.values()];
      
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
   def bytes_recyclebin(
      self
   ) -> float:
      if self._bytes_recyclebin is None:
         self._bytes_recyclebin = 0;
      return self._bytes_recyclebin;
   
   ####
   def gb_recyclebin(
      self
   ) -> float:
      return self.bytes_recyclebin() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(
      self
   ) -> float:
      if self._bytes_comp_none is None:
         self.get_segment_size();
      return self._bytes_comp_none;
      
   ####
   def gb_comp_none(
      self
   ) -> float:
      return self.bytes_comp_none() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(
      self
   ) -> float:
      if self._bytes_comp_low is None:
         self.get_segment_size();
      return self._bytes_comp_low;
      
   ####
   def gb_comp_low(
      self
   ) -> float:
      return self.bytes_comp_low() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(
      self
   ) -> float:
      if self._bytes_comp_high is None:
         self.get_segment_size();
      return self._bytes_comp_high;
      
   ####
   def gb_comp_high(
      self
   ) -> float:
      return self.bytes_comp_high() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(
      self
   ) -> float:
      if self._bytes_comp_unk is None:
         self.get_segment_size();
      return self._bytes_comp_unk;
      
   ####
   def gb_comp_unk(
      self
   ) -> float:
      return self.bytes_comp_unk() / 1024 / 1024 / 1024;
      
   ####
   def bytes_resizeable(
      self
   ) -> float:
      
      rez = 0;
      for item in self.datafiles_l:
         rez += item.bytes_resizeable();         
      return rez;
   
   ####
   def gb_resizeable(
      self
   ) -> float:
      return self.bytes_resizeable() / 1024 / 1024 / 1024;
      
   ############################################################################
   def get_segment_size(
      self
   ):
      curs = self._sqliteconn.cursor();
      
      str_sql = """
         SELECT
          a.tablespace_name
         ,CASE
          WHEN b.bytes_used IS NULL
          THEN
            0
          ELSE
            b.bytes_used
          END AS bytes_used
         ,CASE
          WHEN b.bytes_comp_none IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_none
          END AS bytes_comp_none
         ,CASE
          WHEN b.bytes_comp_low IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_low
          END AS bytes_comp_low
         ,CASE
          WHEN b.bytes_comp_high IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_high
          END AS bytes_comp_high
         ,CASE
          WHEN b.bytes_comp_unk IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_unk
          END AS bytes_comp_unk
         FROM 
         dba_tablespaces a
         LEFT JOIN (
            SELECT
             bb.tablespace_name
            ,SUM(bb.bytes_used) AS bytes_used
            ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
            ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
            ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
            ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
            FROM
            segments_compression bb
            GROUP BY
            bb.tablespace_name
         ) b
         ON
         a.tablespace_name = b.tablespace_name
         WHERE
         a.tablespace_name = :p01
      """;
         
      curs.execute(
          str_sql
         ,{'p01':self._tablespace_name}    
      );
      
      for row in curs:
         tablespace_name = row[0];
         bytes_used      = row[1];
         bytes_comp_none = row[2];
         bytes_comp_low  = row[3];
         bytes_comp_high = row[4];
         bytes_comp_unk  = row[5];
      
      self._bytes_comp_none = bytes_comp_none
      self._bytes_comp_low  = bytes_comp_low
      self._bytes_comp_high = bytes_comp_high
      self._bytes_comp_unk  = bytes_comp_unk
            
      curs.close();
     
   ####
   def move_from_hw(
       self
      ,target_tablespace_name: str
      ,df_result_count       : int   = None
      ,df_result_gb          : float = None
      ,rebuild_spatial       : bool  = True
   ) -> list[DDL]:
   
      rez = [];
      for df in self.datafiles_l:
         
         rez = rez + df.move_from_hw(
            target_tablespace_name = target_tablespace_name
           ,result_count           = df_result_count
           ,result_gb              = df_result_gb
           ,rebuild_spatial        = rebuild_spatial
         );
         
      curs = self._sqliteconn.cursor();
      
      str_sql = """
         CREATE TEMP TABLE tmp_hw_moves(
             priority_num      INTEGER
            ,owner             TEXT
            ,segment_name      TEXT
            ,partition_name    TEXT
            ,segment_type      TEXT
            ,ddl_rebuild       BOOLEAN
            ,ddl_move          BOOLEAN
            ,ddl_recreate      BOOLEAN
            ,statements        TEXT
            ,keep_flag         BOOLEAN
         );
      """;
      
      curs.execute(str_sql);
      
      str_sql = """
         INSERT INTO tmp_hw_moves(
             priority_num
            ,owner
            ,segment_name
            ,partition_name
            ,segment_type
            ,ddl_rebuild
            ,ddl_move
            ,ddl_recreate
            ,statements
         ) VALUES (
            ?,?,?,?,?,?,?,?,?
         )
      """;
      
      for item in rez:
         curs.execute(
             str_sql
            ,(
                item.priority_num
               ,item.owner
               ,item.segment_name
               ,item.partition_name
               ,item.segment_type
               ,item.ddl_rebuild
               ,item.ddl_move
               ,item.ddl_recreate
               ,item.statements_str()
             )          
         );
      
      str_sql = """
         SELECT
         a.owner
         FROM
         tmp_hw_moves a
         WHERE
         a,owner != 'free'
         GROUP BY
         a.owner
      """;
      
      curs.execute(str_sql);
      
      for row in curs:
         print(row[0])
      
      return rez;
      