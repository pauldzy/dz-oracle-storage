import os,sys;

############################################################################### 
class Tablespace(object):

   def __init__(
       self
      ,parent
      ,tablespace_name
      ,bytes_allocated
      ,bytes_used
      ,bytes_free
   ):
   
      self._parent           = parent;
      self._sqliteconn       = parent._sqliteconn;
      self._tablespace_name  = tablespace_name;
      self._bytes_allocated  = bytes_allocated;
      self._bytes_used       = bytes_used;
      
      # bytes free may or may not include recyclebin
      self._bytes_free       = bytes_free;
      self._bytes_recyclebin = None;
      
      self._bytes_comp_none  = None;
      self._bytes_comp_low   = None;
      self._bytes_comp_high  = None;
      self._bytes_comp_unk   = None;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
      
   ####
   def bytes_allocated(self):
      return self._bytes_allocated;
      
   ####
   def gb_allocated(self):
      return self.bytes_allocated() / 1024 / 1024 / 1024;
   
   ####
   def bytes_used(self):
      return self._bytes_used;
      
   ####
   def gb_used(self):
      return self.bytes_used() / 1024 / 1024 / 1024;

   ####
   def bytes_free(self):
      return self._bytes_free;
   
   ####
   def gb_free(self):
      return self.bytes_free() / 1024 / 1024 / 1024;
      
   ####
   def bytes_recyclebin(self):
      if self._bytes_recyclebin is None:
         self._bytes_recyclebin = 0;
      return self._bytes_recyclebin;
   
   ####
   def gb_recyclebin(self):
      return self.bytes_recyclebin() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(self):
      if self._bytes_comp_none is None:
         self.get_segment_size();
      return self._bytes_comp_none;
      
   ####
   def gb_comp_none(self):
      return self.bytes_comp_none() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(self):
      if self._bytes_comp_low is None:
         self.get_segment_size();
      return self._bytes_comp_low;
      
   ####
   def gb_comp_low(self):
      return self.bytes_comp_low() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(self):
      if self._bytes_comp_high is None:
         self.get_segment_size();
      return self._bytes_comp_high;
      
   ####
   def gb_comp_high(self):
      return self.bytes_comp_high() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(self):
      if self._bytes_comp_unk is None:
         self.get_segment_size();
      return self._bytes_comp_unk;
      
   ####
   def gb_comp_unk(self):
      return self.bytes_comp_unk() / 1024 / 1024 / 1024;
      
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
     
