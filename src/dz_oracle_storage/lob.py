import os,sys;
from .index import Index;
from .util  import dzq;

############################################################################### 
class Lob(object):

   def __init__(
       self
      ,parent
      ,owner            : str
      ,table_name       : str
      ,column_name      : str
      ,segment_name     : str
      ,tablespace_name  : str
      ,index_name       : str
      ,compression      : str
      ,src_compression  : str
      ,securefile       : str
      ,varray_type_owner: str
      ,varray_type_name : str
   ):
   
      self._parent             = parent;
      self._sqliteconn         = parent._sqliteconn;
      self._owner              = owner;
      self._table_name         = table_name;
      self._column_name        = column_name;
      self._segment_name       = segment_name;
      self._tablespace_name    = tablespace_name;
      self._index_name         = index_name;
      self._compression        = compression;
      self._src_compression    = src_compression;
      self._securefile         = securefile;
      self._varray_type_owner  = varray_type_owner;
      self._varray_type_name   = varray_type_name;
      
   @property
   def owner(self):
      return dzq(self._owner);
 
   @property
   def table_name(self):
      return dzq(self._table_name);
      
   @property
   def column_name(self):
      return dzq(self._column_name);
            
   @property
   def segment_name(self):
      return dzq(self._segment_name);
            
   @property
   def tablespace_name(self):
      return dzq(self._tablespace_name)
                  
   @property
   def index_name(self):
      return dzq(self._index_name);
                  
   @property
   def compression(self):
      return self._compression;
      
   @property
   def src_compression(self):
      return self._src_compression;
                  
   @property
   def securefile(self):
      return self._securefile;
      
   @property
   def varray_type_owner(self):
      return dzq(self._varray_type_owner);
      
   @property
   def varray_type_name(self):
      return dzq(self._varray_type_name);
      
   ###
   def compression_text(
       self
      ,set_compression: str    
   ):
      
      if set_compression in ['NONE']:
         return "NOCOMPRESS";
      elif set_compression in ['LOW']:
         return "COMPRESS LOW";
      elif set_compression in ['MEDIUM']:
         return "COMPRESS MEDIUM";
      elif set_compression in ['HIGH']:
         return "COMPRESS HIGH";       
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
      and set_compression != self.compression \
      and self.securefile == 'YES':
         prms += self.compression_text(set_compression) + ' ';
         
      if move_tablespace is not None \
      and move_tablespace != self.tablespace_name:
         prms += "TABLESPACE " + move_tablespace + ' ';
         
      if len(prms) > 0:
         prms = '(' + prms.strip() + ')';
         
      scfl = "";
      if self.securefile == 'YES':
         scfl = "SECUREFILE ";

      if self.varray_type_owner is not None:
         rez.append('ALTER TABLE ' + self.owner + '.' + self.table_name + ' ' \
            + 'MOVE VARRAY ' + self.column_name + ' '                         \
            + 'STORE AS ' + scfl + 'LOB' + prms + ';');
      
      else:
         rez.append('ALTER TABLE ' + self.owner + '.' + self.table_name + ' ' \
            + 'MOVE LOB(' + self.column_name + ') '                           \
            + 'STORE AS ' + scfl + 'LOB' + prms + ';');
      
      return rez;
      