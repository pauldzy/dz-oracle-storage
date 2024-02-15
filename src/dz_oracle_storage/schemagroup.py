import os,sys;

from .util import dzx;

############################################################################### 
class SchemaGroup(object):

   def __init__(
       self
      ,parent
      ,schema_group_name
   ):
   
      self._parent              = parent
      self._schema_group_name   = schema_group_name;
      self._schemas             = {};
      self._ignore_tbs          = None;
      
   @property
   def schema_group_name(self):
      return self._schema_group_name;
      
   @property
   def schemas(self):
      return self._schemas;
      
   @property
   def schemas_l(self):
      return [d for d in self.schemas.values()];
      
   ####
   def bytes_used(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_used(igtbs=igtbs);
      return rez;
      
   ####
   def gb_used(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_used(igtbs=igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_none(igtbs=igtbs);
      return rez;
      
   ####
   def gb_comp_none(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_none(igtbs=igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_low(igtbs=igtbs);
      return rez;
      
   ####
   def gb_comp_low(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_low(igtbs=igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_high(igtbs=igtbs);
      return rez;
      
   ####
   def gb_comp_high(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_high(igtbs=igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_unk(igtbs=igtbs);
      return rez;
      
   ####
   def gb_comp_unk(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_comp_unk(igtbs=igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_recyclebin(
       self
      ,igtbs = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.schemas.values():
         rez += item.recyclebin(igtbs=igtbs);
      return rez;
      
   ####
   def gb_recyclebin(
       self
      ,igtbs = None
   ) -> float:
      return self.bytes_recyclebin(igtbs) / 1024 / 1024 / 1024;
      
   ############################################################################
   def add_schema(
       self
      ,schema_name
   ):
   
      if self._schemas is None:
         self._schemas = {};
         
      if schema_name not in self._parent.schemas:
         raise Exception(schema_name + " not found in instance schemas.");
      
      self._schemas[schema_name] = self._parent.schemas[schema_name];
      
   ############################################################################
   def delete_schema(
       self
      ,schema_name
   ):
   
      if self._schemas is None:
         self._schemas = {};

      if schema_name in self._schemas:
         del self._schemas[schema_name];
         
   ############################################################################
   def set_ignore_tablespaces(
       self
      ,tablespace_names
   ):
   
      self._ignore_tbs = dzx(tablespace_names);
  
   ############################################################################
   def delete_ignore_tablespaces(
       self
   ):
   
      del self._ignore_tbs;
      
