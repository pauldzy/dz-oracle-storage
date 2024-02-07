import os,sys;

############################################################################### 
class TablespaceGroup(object):

   def __init__(
       self
      ,parent
      ,tablespace_group_name
   ):
   
      self._parent                = parent;
      self._tablespace_group_name = tablespace_group_name;
      self._tablespaces           = {};
      
   @property
   def tablespace_group_name(self):
      return self._tablespace_group_name;
      
   @property
   def tablespaces(self):
      return self._tablespaces;
      
   @property
   def tablespaces_l(self):
      return [d for d in self.tablespaces.values()];

   ####
   def bytes_allocated(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_allocated();
      return rez;
      
   ####
   def gb_allocated(self):
      return self.bytes_allocated() / 1024 / 1024 / 1024;
   
   ####
   def bytes_used(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_used();
      return rez;
      
   ####
   def gb_used(self):
      return self.bytes_used() / 1024 / 1024 / 1024;

   ####
   def bytes_free(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_free();
      return rez;
   
   ####
   def gb_free(self):
      return self.bytes_free() / 1024 / 1024 / 1024;
      
   ####
   def bytes_recyclebin(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_recyclebin();
      return rez;
   
   ####
   def gb_recyclebin(self):
      return self.bytes_recyclebin() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_comp_none();
      return rez;
      
   ####
   def gb_comp_none(self):
      return self.bytes_comp_none() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_comp_low();
      return rez;
      
   ####
   def gb_comp_low(self):
      return self.bytes_comp_low() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_comp_high();
      return rez;
      
   ####
   def gb_comp_high(self):
      return self.bytes_comp_high() / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_comp_unk();
      return rez;
      
   ####
   def gb_comp_unk(self):
      return self.bytes_comp_unk() / 1024 / 1024 / 1024;
      
   ############################################################################
   def add_tablespace(
       self
      ,tablespace_name
   ):
   
      if self._tablespaces is None:
         self._tablespaces = {};
         
      self._tablespaces[tablespace_name] = self._parent.tablespaces[tablespace_name];
      
   ############################################################################
   def delete_tablespace(
       self
      ,tablespace_name
   ):
   
      if self._tablespaces is None:
         self._tablespaces = {};

      if tablespace_name in self._tablespaces:
         del self._tablespaces[tablespace_name];
         
