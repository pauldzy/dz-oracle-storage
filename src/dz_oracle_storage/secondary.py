import os,sys;

############################################################################### 
class Secondary(object):

   def __init__(
       self
      ,parent_resource
      ,depth
      ,owner
      ,segment_name
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
      ,index_type       = None
      ,ityp_owner       = None
      ,ityp_name        = None
      ,partitioned      = None
      ,iot_type         = None
      ,isgeor           = None
   ):
   
      self._parent_resource  = parent_resource;
      self._owner            = owner;
      self._depth            = depth;
      self._segment_name     = segment_name;
      self._partition_name   = partition_name;
      self._segment_type     = segment_type;
      self._tablespace_name  = tablespace_name;
      self._compression      = compression;
      self._src_compression  = src_compression;
      self._src_compress_for = src_compress_for;
      
      if bytes_used is None:
         self._bytes_used   = 0;
      else:
         self._bytes_used   = bytes_used;
      
      if bytes_comp_none is None:
         self._bytes_comp_none = 0;
      else:
         self._bytes_comp_none = bytes_comp_none;
      
      if bytes_comp_low is None:
         self._bytes_comp_low = 0;
      else:
         self._bytes_comp_low  = bytes_comp_low;
      
      if bytes_comp_high is None:
         self._bytes_comp_high = 0;
      else:
         self._bytes_comp_high = bytes_comp_high;
      
      if bytes_comp_unk is None:
         self._bytes_comp_unk = 0;
      else:
         self._bytes_comp_unk  = bytes_comp_unk;
         
      self._index_type      = index_type;
      self._ityp_owner      = ityp_owner;
      self._ityp_name       = ityp_name;
      self._partitioned     = partitioned;
      self._iot_type        = iot_type;
      self._isgeor          = isgeor;
      
      curs = parent_resource._sqliteconn.cursor();
      
      if self._segment_type in ['NESTED TABLE','TABLE','TABLE PARTITION','TABLE SUBPARTITION'] \
      or (self._segment_type is None and self._iot_type == 'IOT'):
         
         # Harvest all table lob segments and indexes
         str_sql = """
            SELECT
             a.owner
            ,a.segment_name
            ,b.partition_name
            ,b.segment_type
            ,b.tablespace_name
            ,b.compression
            ,b.src_compression
            ,b.bytes_used
            ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
            FROM
            dba_lobs a
            LEFT JOIN
            segments_compression b
            ON
                a.owner        = b.owner
            AND a.segment_name = b.segment_name
            WHERE
                a.owner        = :p01
            AND a.table_name   = :p02
            
            UNION ALL
            
            SELECT
             c.owner
            ,c.index_name
            ,d.partition_name
            ,d.segment_type
            ,d.tablespace_name
            ,d.compression
            ,d.src_compression
            ,d.bytes_used
            ,CASE WHEN d.compression = 'NONE' THEN d.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN d.compression = 'LOW'  THEN d.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN d.compression = 'HIGH' THEN d.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN d.compression = 'UNK'  THEN d.bytes_used ELSE 0 END AS bytes_comp_unk
            FROM
            dba_lobs c
            LEFT JOIN
            segments_compression d
            ON
                c.owner        = d.owner
            AND c.index_name   = d.segment_name
            WHERE
                c.owner        = :p03
            AND c.table_name   = :p04
         """;

         curs.execute(
             str_sql
            ,{
                'p01':self._owner,'p02':self._segment_name
               ,'p03':self._owner,'p04':self._segment_name
             }
         );
         
         for row in curs: 
            owner           = row[0];
            segment_name    = row[1];
            partition_name  = row[2];
            segment_type    = row[3];
            tablespace_name = row[4];
            compression     = row[5];
            src_compression = row[6];
            bytes_used      = row[7];
            bytes_comp_none = row[8];
            bytes_comp_low  = row[9];
            bytes_comp_high = row[10];
            bytes_comp_unk  = row[11];
         
            if (owner,segment_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(owner,segment_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,owner           = owner
                  ,depth           = depth + 1
                  ,segment_name    = segment_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
                  ,compression     = compression
                  ,src_compression = src_compression
                  ,bytes_used      = bytes_used
                  ,bytes_comp_none = bytes_comp_none
                  ,bytes_comp_low  = bytes_comp_low
                  ,bytes_comp_high = bytes_comp_high
                  ,bytes_comp_unk  = bytes_comp_unk
               );
            
         # Harvest all table indexes
         str_sql = """
            SELECT
             a.owner
            ,a.index_name
            ,b.partition_name
            ,b.segment_type
            ,b.tablespace_name
            ,b.compression
            ,b.src_compression
            ,b.bytes_used
            ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
            ,a.index_type
            ,a.ityp_owner
            ,a.ityp_name
            FROM
            dba_indexes a
            LEFT JOIN
            segments_compression b
            ON
                a.owner        = b.owner
            AND a.index_name   = b.segment_name
            WHERE
                a.table_owner = :p01
            AND a.table_name  = :p02
         """;

         curs.execute(
             str_sql
            ,{
               'p01':self._owner,'p02':self._segment_name
             }
         );
         
         for row in curs:
            index_owner     = row[0];
            index_name      = row[1];
            partition_name  = row[2];
            segment_type    = row[3];
            tablespace_name = row[4];
            compression     = row[5];
            src_compression = row[6];
            bytes_used      = row[7];
            bytes_comp_none = row[8];
            bytes_comp_low  = row[9];
            bytes_comp_high = row[10];
            bytes_comp_unk  = row[11];
            index_type      = row[12];
            ityp_owner      = row[13];
            ityp_name       = row[14];
            
            if (index_owner,index_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(index_owner,index_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,owner           = index_owner
                  ,depth           = depth + 1
                  ,segment_name    = index_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
                  ,compression     = compression
                  ,src_compression = src_compression
                  ,bytes_used      = bytes_used
                  ,bytes_comp_none = bytes_comp_none
                  ,bytes_comp_low  = bytes_comp_low
                  ,bytes_comp_high = bytes_comp_high
                  ,bytes_comp_unk  = bytes_comp_unk
                  ,index_type      = index_type
                  ,ityp_owner      = ityp_owner
                  ,ityp_name       = ityp_name
               );
    
      elif self._index_type == 'DOMAIN':
      
         curs2 = parent_resource._sqliteconn.cursor();
               
         if self._ityp_owner == 'CTXSYS' and self._ityp_name == 'CONTEXT':
            str_sql = """
               SELECT
                a.owner
               ,a.table_name
               ,b.partition_name
               ,b.segment_type
               ,b.tablespace_name
               ,b.compression
               ,b.src_compression
               ,b.src_compress_for
               ,b.bytes_used
               ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
               ,a.iot_type
               ,b.isgeor
               FROM
               dba_tables a
               LEFT JOIN
               segments_compression b
               ON
                   a.owner         = b.owner
               AND a.table_name    = b.segment_name
               WHERE
                   a.owner         = :p01
               AND a.table_name LIKE :p02
            """;
            
            curs.execute(
                str_sql
               ,{
                  'p01':self._owner,'p02':'DR$' + self._segment_name + '$%'
                }
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
               iot_type         = row[13];
               isgeor           = row[14];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource  = parent_resource
                     ,depth            = depth + 1
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
                     ,iot_type         = iot_type
                     ,isgeor           = isgeor
                  );

         elif self._ityp_owner == 'MDSYS' and self._ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
            str_sql = """
               SELECT
                a.sdo_index_owner
               ,a.sdo_index_table
               FROM
               sdo_index_metadata_table a
               WHERE
                   a.sdo_index_owner = :p01
               AND a.sdo_index_name  = :p02
            """;
            
            curs.execute(
                str_sql
               ,{
                  'p01':self._owner,'p02':self._segment_name
                }
            );
            
            domain_stub = None;
            for row in curs:
               domain_owner = row[0];
               domain_stub  = row[1];
               
            if domain_stub is None:
               raise Exception('mdsys spatial index error');
            
            str_sql = """
               SELECT
                a.owner
               ,a.table_name
               ,b.partition_name
               ,b.segment_type
               ,b.tablespace_name
               ,b.compression
               ,b.src_compression
               ,b.src_compress_for
               ,b.bytes_used
               ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
               ,a.iot_type
               ,b.isgeor
               FROM
               dba_tables a
               LEFT JOIN
               segments_compression b
               ON
                   a.owner        = b.owner
               AND a.table_name   = b.segment_name
               WHERE
                   a.owner        = :p01
               AND a.table_name IN (:p02,:p03,:p04)
            """;
            
            curs.execute(
                str_sql
               ,{
                   'p01':self._owner
                  ,'p02':domain_stub
                  ,'p03':domain_stub.replace("MDRT","MDXT")
                  ,'p04':domain_stub.replace("MDRT","MDTP")
                }
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
               iot_type         = row[13];
               isgeor           = row[14];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource  = parent_resource
                     ,depth            = depth + 1
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
                     ,iot_type         = iot_type
                     ,isgeor           = isgeor
                  );

         elif self._ityp_owner == 'SDE' and self._ityp_name == 'ST_SPATIAL_INDEX':
            str_sql = """
               SELECT
               a.index_id
               FROM
               sde_st_geometry_index a
               WHERE
                   a.owner      = :p01
               AND a.index_name = :p02 
            """;

            curs.execute(
                str_sql
               ,{
                  'p01':self._owner,'p02':self._segment_name
                }
            );
            
            domain_stub = None;
            for row in curs:
               domain_stub = row[0];
               
            if domain_stub is None:
               raise Exception('sde spatial index error');
               
            str_sql = """
               SELECT
                a.owner
               ,a.table_name
               ,b.partition_name
               ,b.segment_type
               ,b.tablespace_name
               ,b.compression
               ,b.src_compression
               ,b.src_compress_for
               ,b.bytes_used
               ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
               ,a.iot_type
               ,b.isgeor
               FROM
               dba_tables a
               LEFT JOIN
               segments_compression b
               ON
                   a.owner        = b.owner
               AND a.table_name   = b.segment_name
               WHERE
                   a.owner        = :p01
               AND a.table_name   = :p02
            """;
            
            curs.execute(
                str_sql
               ,{
                   'p01':self._owner
                  ,'p02':'S' + domain_stub + '_IDX$'
                }
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
               iot_type         = row[13];
               isgeor           = row[14];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource  = parent_resource
                     ,depth            = depth + 1
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
                     ,iot_type         = iot_type
                     ,isgeor           = isgeor
                  );
            
         else:
            sys.stderr.write('unhandled domain index: ' + str(ityp_owner) + '.' + str(ityp_name));            
   
         curs2.close(); 
         
      #########################################################################
      # Run down each partition
      if self._partitioned == 'YES':
         str_sql = """
            SELECT
             a.table_owner
            ,a.table_name
            ,a.partition_name
            ,b.segment_type
            ,a.tablespace_name
            ,b.compression
            ,b.src_compression
            ,b.src_compress_for
            ,b.bytes_used
            ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
            FROM
            dba_tab_partitions a
            LEFT JOIN
            segments_compression b
            ON
                a.table_owner  = b.owner
            AND a.table_name   = b.segment_name
            AND a.partition_name = b.partition_name
            WHERE
                a.table_owner  = :p01
            AND a.table_name   = :p02
         """;
         
         curs.execute(
             str_sql
            ,{
                'p01':self._owner
               ,'p02':self._segment_name
             }
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
            
            if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                   parent_resource  = parent_resource
                  ,depth            = depth + 1
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
               );
 
      #########################################################################
      # Run down each raster RDT component
      if self._isgeor == 'GEOR':
         str_sql = """
            SELECT
             a.owner
            ,a.rdt_table_name
            ,b.partition_name
            ,b.segment_type
            ,b.tablespace_name
            ,b.compression
            ,b.src_compression
            ,b.src_compress_for
            ,b.bytes_used
            ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
            ,b.isgeor
            FROM
            all_sdo_geor_sysdata a
            LEFT JOIN
            segments_compression b
            ON
                a.owner          = b.owner
            AND a.rdt_table_name = b.segment_name
            WHERE
                a.owner          = :p01
            AND a.table_name     = :p02
         """;
         
         curs.execute(
             str_sql
            ,{
                'p01':self._owner
               ,'p02':self._segment_name
             }
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
            isgeor           = row[13];
            
            if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                   parent_resource  = parent_resource
                  ,depth            = depth + 1
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
                  ,isgeor           = isgeor
               );
      
      curs.close();
      
   @property
   def name(self):
      if self._partition_name is None:
         return self._owner + '.' + self._segment_name;
      else:
         return self._owner + '.' + self._segment_name + '.' + self._partition_name;
         
   @property
   def depth(self):
      return self._depth;
      
   @property
   def owner(self):
      return self._owner;
      
   @property
   def segment_name(self):
      return self._segment_name;
      
   @property
   def partition_name(self):
      return self._partition_name;
      
   @property
   def segment_type(self):
      return self._segment_type;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
      
   @property
   def compression(self):
      return self._compression;
      
   @property
   def src_compression(self):
      return self._src_compression;
  
   @property
   def src_compress_for(self):
      return self._src_compress_for;
      
   ####
   def bytes_used(
       self
      ,igtbs = None
   ):
      return self._bytes_used;
      
   ####
   def gb_used(
       self
      ,igtbs = None
   ):
      return self.bytes_used(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_none(
       self
      ,igtbs = None
   ):
      return self._bytes_comp_none;
      
   ####
   def gb_comp_none(
       self
      ,igtbs = None
   ):
      return self.bytes_comp_none(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_low(
       self
      ,igtbs = None
   ):
      return self._bytes_comp_low;
      
   ####
   def gb_comp_low(
       self
      ,igtbs = None
   ):
      return self.bytes_comp_low(igtbs) / 1024 / 1024 / 1024;
      
   ####
   def bytes_comp_high(
       self
      ,igtbs = None
   ):
      return self._bytes_comp_high;
   
   ####
   def gb_comp_high(
       self
      ,igtbs = None
   ):
      return self.bytes_comp_high(igtbs) / 1024 / 1024 / 1024;
   
   ####
   def bytes_comp_unk(
       self
      ,igtbs = None
   ):
      return self._bytes_comp_unk;
      
   ####
   def gb_comp_unk(
       self
      ,igtbs = None
   ):
      return self.bytes_comp_unk(igtbs) / 1024 / 1024 / 1024;
