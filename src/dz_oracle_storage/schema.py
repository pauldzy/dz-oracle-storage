import os,sys;
from .util import dzx,dzq;

############################################################################### 
class Schema(object):

   def __init__(
       self
      ,parent
      ,schema_name    : str
      ,bytes_used     : float
      ,bytes_comp_none: float
      ,bytes_comp_low : float
      ,bytes_comp_high: float
      ,bytes_comp_unk : float
   ):
   
      self._parent             = parent;
      self._sqliteconn         = parent._sqliteconn;
      self._schema_name        = schema_name;
      self._bytes_used         = bytes_used;
      self._bytes_comp_none    = bytes_comp_none;
      self._bytes_comp_low     = bytes_comp_low;
      self._bytes_comp_high    = bytes_comp_high;
      self._bytes_comp_unk     = bytes_comp_unk;
      self._bytes_recyclebin   = 0;
      self._ignore_tbs_results = {};
      
   @property
   def schema_name(self):
      return dzq(self._schema_name);
      
   ####
   def bytes_used(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_used'];
         
      return self._bytes_used;
      
   ####
   def gb_used(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_used(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_comp_none'];
         
      return self._bytes_comp_none;
      
   ####
   def gb_comp_none(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_none(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_comp_low'];
         
      return self._bytes_comp_low;
      
   ####
   def gb_comp_low(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_low(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_comp_high'];
         
      return self._bytes_comp_high;
      
   ####
   def gb_comp_high(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_high(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_comp_unk'];
         
      return self._bytes_comp_unk;
      
   ####
   def gb_comp_unk(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_unk(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_recyclebin(
       self
      ,igtbs = None
   ) -> float:
      if dzx(igtbs) is not None:
         if dzx(igtbs) not in self._ignore_tbs_results:
            self.reharvest_with_ignore(dzx(igtbs));

         return self._ignore_tbs_results[dzx(igtbs)]['bytes_recyclebin'];
         
      return self._bytes_recyclebin;
      
   ####
   def gb_recyclebin(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_recyclebin(igtbs) / 1024 / 1024 / 1024;
      
   ############################################################################
   def reharvest_with_ignore(
       self
      ,igtbs_s
   ):
      if igtbs_s is None or igtbs_s in self._ignore_tbs_results:
         return;
 
      curs = self._sqliteconn.cursor();
      
      str_sql = """
         SELECT
          a.username
          
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
         dba_users a
         LEFT JOIN (
            SELECT
             bb.owner
            ,SUM(bb.bytes_used) AS bytes_used
            ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
            ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
            ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
            ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
            FROM
            segments_compression bb
            WHERE
            bb.tablespace_name NOT IN (""" + igtbs_s + """)
            GROUP BY
            bb.owner
         ) b
         ON
         a.username = b.owner
         WHERE
         a.username = :p01
      """;
  
      curs.execute(
          str_sql
         ,{'p01':self._schema_name}    
      );
      for row in curs:
         schema_name     = row[0];
         bytes_used      = row[1];
         bytes_comp_none = row[2];
         bytes_comp_low  = row[3];
         bytes_comp_high = row[4];
         bytes_comp_unk  = row[5];            
      
      self._ignore_tbs_results[igtbs_s] = {
          "bytes_used":       bytes_used
         ,"bytes_comp_none":  bytes_comp_none
         ,"bytes_comp_low":   bytes_comp_low
         ,"bytes_comp_high":  bytes_comp_high
         ,"bytes_comp_unk":   bytes_comp_unk
         ,"bytes_recyclebin": 0
      }
      
      str_sql = """
         SELECT
          a.owner
         ,SUM(b.bytes) AS bytes_recyclebin
         FROM (
            SELECT
             aa.owner
            ,aa.object_name
            ,aa.ts_name
            FROM
            dba_recyclebin aa
            WHERE
                aa.ts_name IS NOT NULL
            AND aa.ts_name NOT IN (""" + igtbs_s + """)
            GROUP BY
             aa.owner
            ,aa.object_name
            ,aa.ts_name
         ) a
         JOIN
         dba_segments b
         ON
             a.owner       = b.owner
         AND a.object_name = b.segment_name
         WHERE 
         a.owner = :p01
         GROUP BY
         a.owner
      """;
      
      curs.execute(
          str_sql
         ,{'p01':self._schema_name}    
      );
      bytes_recyclebin = None;
      for row in curs:
         owner            = row[0];
         bytes_recyclebin = row[1];
       
      if bytes_recyclebin is not None:       
         self._ignore_tbs_results[igtbs_s]['bytes_recyclebin'] = bytes_recyclebin;
                 
      curs.close();
      