import os,sys;

from .secondary import Secondary;

############################################################################### 
class Resource(object):

   def __init__(
       self
      ,parent
      ,table_owner     :str
      ,table_name      :str
      ,partition_name  :str = None
      ,segment_type    :str = None
      ,tablespace_name :str = None
      ,compression     :str = None
      ,src_compression :str = None
      ,src_compress_for:str = None
      ,bytes_used      :int = None
      ,bytes_comp_none :int = None
      ,bytes_comp_low  :int = None
      ,bytes_comp_high :int = None
      ,bytes_comp_unk  :int = None
      ,partitioned     :str = None
      ,iot_type        :str = None
      ,temporary       :str = None
      ,secondary       :str = None
      ,isgeor          :str = None
   ):
    
      self._parent           = parent;
      self._sqliteconn       = parent._sqliteconn;
      self._table_owner      = table_owner;
      self._table_name       = table_name;
      self._partition_name   = partition_name;
      self._segment_type     = segment_type;
      self._tablespace_name  = tablespace_name;
      self._compression      = compression;
      self._src_compression  = src_compression;
      self._src_compress_for = src_compress_for;
      
      self._partitioned      = partitioned;
      self._iot_type         = iot_type;
      self._temporary        = temporary;
      self._secondary        = secondary;      
      self._isgeor           = isgeor;
      
      self._secondaries      = {};
      
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
      ,recipe           : str
      ,rebuild_spatial  : bool = False
      ,move_tablespace  : str  = None
      ,set_compression  : str  = None
   ) -> list[str]:
      rez = [];
      rebuild_indx_flg = False;
    
      if recipe in ['REBUILDSPX']:
         rebuild_spatial = True;
      
      if recipe in ['HIGH','SHRINKSFLOB']:
         for item in self.secondaries.values():
            
            if  item.segment_type == 'LOBSEGMENT'      \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            ):
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_indx_flg = rebuild_indx_flg
                  ,rebuild_spatial  = rebuild_spatial
                  ,move_tablespace  = move_tablespace
                  ,set_compression  = set_compression
               );
               if r is not None:
                  rebuild_indx_flg = True;
                  rez = rez + r;
                  
            else:
               rebuild_indx_flg = False;
      
      if recipe in ['HIGH']:      
         for item in self.secondaries.values():
            
            if  item.segment_type == 'TABLE'           \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            )                                          \
            and item.iot_type is None                  \
            and item.temporary == 'N':
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_indx_flg = rebuild_indx_flg
                  ,rebuild_spatial  = rebuild_spatial
                  ,move_tablespace  = move_tablespace
                  ,set_compression  = set_compression
               );
               if r is not None:
                  rebuild_indx_flg = True;
                  rez = rez + r;
                  
            else:
               rebuild_indx_flg = False;
             
      if recipe in ['HIGH','REBUILDSPX','SHRINKSFLOB']:
         for item in self.secondaries.values():
            
            if  item.segment_type == 'INDEX'           \
            and (                                      \
               item._parent_secondary is None or       \
               item._parent_secondary.secondary == 'N' \
            )                                          \
            and item._parent_secondary.iot_type is None\
            and item._parent_secondary.temporary == 'N':
               
               r = item.generate_ddl(
                   recipe = recipe
                  ,rebuild_indx_flg = rebuild_indx_flg
                  ,rebuild_spatial  = rebuild_spatial
                  ,move_tablespace  = move_tablespace
                  ,set_compression  = set_compression
               );
               if r is not None:
                  rez = rez + r;
               
      return rez;
      
      