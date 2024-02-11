import os,sys;

from .resource import Resource;

############################################################################### 
class ResourceGroup(object):

   def __init__(
       self
      ,parent
      ,resource_group_name: str
   ):
   
      self._parent              = parent;
      self._sqliteconn          = parent._sqliteconn;
      self._resource_group_name = resource_group_name;
      self._resources           = {};
      self._ignore_tbs          = None;
      
   @property
   def dataset_name(self):
      return self._dataset_name;
      
   @property
   def resources(self):
      return self._resources;
      
   @property
   def resources_l(self):
      return [d for d in self.resources.values()];
      
   ####
   def bytes_used(
       self
      ,igtbs: list = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_used(igtbs);
      return rez;
      
   ####
   def gb_used(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_used(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(
       self
      ,igtbs: list = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_none(igtbs);
      return rez;
      
   ####
   def gb_comp_none(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_comp_none(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(
       self
      ,igtbs: list = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_low(igtbs);
      return rez;
      
   ####
   def gb_comp_low(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_comp_low(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(
       self
      ,igtbs: list = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_high(igtbs);
      return rez;
      
   ####
   def gb_comp_high(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_comp_high(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_unk(
       self
      ,igtbs: list = None
   ) -> float:
      if igtbs is None and self._ignore_tbs is not None:
         igtbs = self._ignore_tbs;
         
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_unk(igtbs);
      return rez;
      
   ####
   def gb_comp_unk(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_comp_unk(igtbs) / 1024 / 1024 / 1024;
      
   ############################################################################
   def add_resource(
       self
      ,table_owner: str
      ,table_name: str
   ):
   
      if self._resources is None:
         self._resources = {};
         
      self._resources[(table_owner,table_name)] = Resource(
          parent       = self
         ,table_owner  = table_owner
         ,table_name = table_name
      );
      
   ############################################################################
   def delete_resource(
       self
      ,table_owner: str
      ,table_name: str
   ):
   
      if self._resources is None:
         self._resources = {};

      if (table_owner,table_name) in self._resources:
         del self._resources[(table_owner,table_name)];
         
   ############################################################################
   def set_ignore_tablespaces(
       self
      ,tablespace_names: list
   ):
   
      self._ignore_tbs = dzx(tablespace_name);
  
   ############################################################################
   def delete_ignore_tablespaces(
       self
   ):
   
      del self._ignore_tbs;
      
   ############################################################################
   def load_resources_from_schema(
       self
      ,schema_name: str
      ,exclude_types: list = None
   ):
   
      if schema_name not in self._parent.schemas:
         raise Exception("schema " + str(schema_name) + " not found in instance.");
         
      curs = self._sqliteconn.cursor();
   
      str_sql = """
         SELECT
          a.owner
         ,a.table_name
         ,a.partition_name
         ,a.segment_type
         ,a.tablespace_name
         ,a.compression
         ,a.src_compression
         ,a.src_compress_for
         ,a.bytes_used
         ,a.bytes_comp_none
         ,a.bytes_comp_low
         ,a.bytes_comp_high
         ,a.bytes_comp_unk
         ,a.partitioned
         ,a.iot_type
         ,a.temporary
         ,a.secondary
         ,a.isgeor
         FROM
         resource_eligible a
         WHERE
         a.owner = :p01
      """;
      
      curs.execute(
          str_sql
         ,{'p01':schema_name}
      );

      for row in curs:
         table_owner      = row[0];
         table_name       = row[1];
         partition_name   = row[2];
         segment_type     = row[3];
         tablespace_name  = row[4];
         compression      = row[5];         
         src_compression  = row[6];
         src_compress_for = row[7];
         bytes_used       = row[8];
         bytes_comp_none  = row[9];
         bytes_comp_low   = row[10];
         bytes_comp_high  = row[11];
         bytes_comp_unk   = row[12];
         partitioned      = row[13];
         iot_type         = row[14];
         temporary        = row[15];
         secondary        = row[16];
         isgeor           = row[17];

         self._resources[(table_owner,table_name)] = Resource(
             parent           = self
            ,table_owner      = table_owner
            ,table_name       = table_name
            ,partition_name   = partition_name
            ,segment_type     = segment_type
            ,tablespace_name  = tablespace_name
            ,compression      = compression            
            ,src_compression  = src_compression
            ,src_compress_for = src_compress_for
            ,bytes_used       = bytes_used
            ,bytes_comp_none  = bytes_comp_none
            ,bytes_comp_low   = bytes_comp_low
            ,bytes_comp_high  = bytes_comp_high
            ,bytes_comp_unk   = bytes_comp_unk
            ,partitioned      = partitioned
            ,iot_type         = iot_type
            ,temporary        = temporary
            ,secondary        = secondary
            ,isgeor           = isgeor
         );
         
         