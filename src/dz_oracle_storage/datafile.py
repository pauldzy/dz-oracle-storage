import os,sys;

############################################################################### 
class Datafile(object):

   def __init__(
       self
      ,parent
      ,file_name
      ,file_id
      ,tablespace_name
      ,bytes_allocated
      ,bytes_free
      ,bytes_largest
      ,extents_hmw   = None
      ,db_block_size = None
   ):
   
      self._parent             = parent;
      self._file_name          = file_name;
      self._file_id            = file_id;
      self._tablespace_name    = tablespace_name;
      self._bytes_allocated    = bytes_allocated;
      self._bytes_free         = bytes_free;
      self._bytes_largest      = bytes_largest;
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
      return self._bytes_allocated - self._bytes_free;
      
   ####
   def gb_used(
      self
   ) -> float:
      return self.bytes_used() / 1024 / 1024 / 1024;

   ####
   def bytes_free(
      self
   ) -> float:
      return self._bytes_allocated - (self._bytes_allocated - self._bytes_free);
   
   ####
   def gb_free(
      self
   ) -> float:
      return self.bytes_free() / 1024 / 1024 / 1024;
      
