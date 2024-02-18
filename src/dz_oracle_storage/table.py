import os,sys;
from .index import Index;

############################################################################### 
class Table(object):

   def __init__(
       self
      ,parent
      ,table_owner    : str
      ,table_name     : str
      ,tablespace_name: str
      ,compression    : str
   ):
   
      self._parent             = parent;
      self._sqliteconn         = parent._sqliteconn;
      self._table_owner        = table_owner;
      self._table_name         = table_name;
      self._tablespace_name    = tablespace_name;
      self._compression        = compression;
      
   @property
   def table_owner(self):
      return self._table_owner;
 
   @property
   def table_name(self):
      return self._table_name;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
 
   @property
   def compression(self):
      return self._compression;
      
   ###
   def compression_text(
       self
      ,set_compression: str    
   ):
      
      if set_compression in ['NONE']:
         return "NOCOMPRESS";
      elif set_compression in ['LOW']:
         return "COMPRESS";
      elif set_compression in ['MEDIUM']:
         return "COMPRESS";
      elif set_compression in ['HIGH']:
         return "COMPRESS FOR OLTP";       
      else:
         raise Exception("unhandled compression " + str(set_compression));
      
   ####
   def rebuild(
       self
      ,set_compression: str  = None
      ,move_tablespace: str  = None
   ) -> list[str]:
   
      rez = [];
      
      prms = "";
      if  set_compression is not None         \
      and set_compression != self.compression :
         prms += self.compression_text(set_compression) + ' ';
         
      if move_tablespace is not None \
      and move_tablespace != self.tablespace_name:
         prms += "TABLESPACE " + move_tablespace + ' ';
         
      if len(prms) > 0:
         prms = prms.strip();

      rez.append('ALTER TABLE ' + self.table_owner + '.' + self.table_name + ' ' \
         + 'MOVE ' + prms + ';');            
      return rez;
      
   ####
   def rebuild_indexes(
       self
      ,rebuild_limited: bool = False
      ,rebuild_spatial: bool = False
      ,set_compression: str  = None
      ,move_tablespace: str  = None
   ) -> list[str]:
   
      curs = self._sqliteconn.cursor();      
      
      str_sql = """
         SELECT
          a.owner
         ,a.index_name
         ,a.index_type
         ,a.table_owner
         ,a.table_name
         ,a.tablespace_name
         ,a.parameters
         ,a.ityp_owner
         ,a.ityp_name
         ,a.index_columns
         ,b.compression
         FROM
         dba_indexes_plus a
         LEFT JOIN
         segments_compression b
         ON
             a.owner       = b.owner
         AND a.index_name  = b.segment_name
         WHERE
             a.table_owner = :p01
         AND a.table_name  = :p02
      """;
      
      curs.execute(
          str_sql
         ,{
             'p01': self.table_owner
            ,'p02': self.table_name
          }
      );
      
      rez = [];
      index_owner      = None;
      index_name       = None;
      index_type       = None;
      table_owner      = None;
      table_name       = None;
      tablespace_name  = None;
      index_parameters = None;
      ityp_owner       = None;
      ityp_name        = None;
      index_columns    = None;
      compression      = None;
      for row in curs:
         index_owner      = row[0];
         index_name       = row[1];
         index_type       = row[2];
         table_owner      = row[3];
         table_name       = row[4];
         tablespace_name  = row[5];
         index_parameters = row[6];
         ityp_owner       = row[7];
         ityp_name        = row[8];
         index_columns    = row[9];
         compression      = row[10];
         
         boo_index = True;
         if index_type == 'BITMAP':
            boo_index = False;
            
         else:
            if rebuild_limited:
               boo_index = False;
               
            if set_compression is not None \
            and set_compression != compression:
               boo_index = True;
               
            if move_tablespace is not None \
            and tablespace_name != move_tablespace:
               boo_index = True;
           
         if boo_index:
            
            rez = rez + Index(
                self
               ,index_owner      = index_owner
               ,index_name       = index_name
               ,index_type       = index_type
               ,table_owner      = table_owner
               ,table_name       = table_name
               ,index_parameters = index_parameters
               ,ityp_owner       = ityp_owner
               ,ityp_name        = ityp_name
               ,index_columns    = index_columns
            ).rebuild(
                rebuild_spatial = rebuild_spatial
               ,set_compression = set_compression
               ,move_tablespace = move_tablespace            
            );
            
      curs.close();
      
      return rez;
      