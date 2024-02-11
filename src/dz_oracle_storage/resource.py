import os,sys;

from .secondary import Secondary;

############################################################################### 
class Resource(object):

   def __init__(
       self
      ,parent
      ,table_owner
      ,table_name
      ,partition_name   = None
      ,segment_type     = None
      ,tablespace_name  = None
      ,compression      = None
      ,src_compression  = None
      ,src_compress_for = None
      ,bytes_used       = None
      ,bytes_comp_none  = None
      ,bytes_comp_low   = None
      ,bytes_comp_high  = None
      ,bytes_comp_unk   = None
      ,partitioned      = None
      ,iot_type         = None
      ,temporary        = None
      ,secondary        = None
      ,isgeor           = None
   ):
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._table_owner     = table_owner;
      self._table_name      = table_name;
      self._temporary       = temporary;
      self._isgeor          = isgeor;
      self._secondaries     = {};
      
      if secondary is None or secondary != 'Y':
         # Verify item is eligible resource item
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
                a.owner         = :p01
            AND a.table_name    = :p02
         """;
         
         curs.execute(
             str_sql
            ,{'p01':table_owner,'p02':table_name}
         );
         table_name = None;
         for row in curs:
            owner            = row[0];
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

         # Abend hard if the item does have seconday = 'N'
         if table_name is None:
            raise Exception(self.table_owner + '.' + self.table_name + ' is not a resource.');     

         self._isgeor = isgeor;
      
      self._secondaries[(table_owner,table_name,partition_name)] = Secondary(
          parent_resource  = self
         ,parent_secondary = None
         ,depth            = 0
         ,owner            = table_owner
         ,segment_name     = table_name
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
            
      curs.close();
   
   @property
   def name(self):
      return self._table_owner + '.' + self._table_name;   
      
   @property
   def table_owner(self):
      return self._table_owner;

   @property
   def table_name(self):
      return self._table_name;
      
   @property
   def temporary(self):
      return self._temporary;
      
   @property
   def secondaries(self):
      return self._secondaries;
      
   @property
   def secondaries_l(self):
      return [d for k,d in sorted(self.secondaries.items())];
      
   ####
   def bytes_used(
       self
      ,igtbs: list = None
   ) -> float:
      rez = 0;
      for item in self.secondaries.values():
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
      rez = 0;
      for item in self.secondaries.values():
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
      rez = 0;
      for item in self.secondaries.values():
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
      rez = 0;
      for item in self.secondaries.values():
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
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_comp_unk(igtbs);
      return rez;
      
   ####
   def gb_comp_unk(
       self
      ,igtbs: list = None
   ) -> float:
      return self.bytes_comp_unk(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def generate_ddl(
       self
      ,recipe: str
   ) -> list[str]:
      rez = [];
      rebuild_trigger = False;
      
      if recipe in ['HIGH']:
         for item in self.secondaries.values():
            if  item.segment_type == 'LOBSEGMENT'      \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            ):
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_trigger = rebuild_trigger
               );
               if r is not None:
                  rebuild_trigger = True;
                  rez = rez + r;
      
      if recipe in ['HIGH']:      
         for item in self.secondaries.values():
            
            if  item.segment_type == 'TABLE'           \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            )                                          \
            and item.iot_type is None:
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_trigger = rebuild_trigger
               );
               if r is not None:
                  rebuild_trigger = True;
                  rez = rez + r;
             
      if recipe in ['HIGH','REBUILDSPX']:
         for item in self.secondaries.values():
            
            if  item.segment_type == 'INDEX'           \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            )                                          \
            and item._parent_secondary.iot_type is None:
    
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_trigger = rebuild_trigger
               );
               if r is not None:
                  rez = rez + r;
               
      return rez;
      
      