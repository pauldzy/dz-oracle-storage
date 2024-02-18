import os,sys;
from .table import Table;
from .index import Index;
from .lob   import Lob;

############################################################################### 
class Secondary(object):

   def __init__(
       self
      ,parent_resource
      ,parent_secondary
      ,depth            :int 
      ,owner            :str
      ,segment_name     :str
      ,partition_name   :str = None
      ,segment_type     :str = None
      ,tablespace_name  :str = None
      ,compression      :str = None
      ,src_compression  :str = None
      ,src_compress_for :str = None
      ,bytes_used       :int = None
      ,bytes_comp_none  :int = None
      ,bytes_comp_low   :int = None
      ,bytes_comp_high  :int = None
      ,bytes_comp_unk   :int = None
      ,index_type       :str = None
      ,index_parameters :str = None
      ,index_table_owner:str = None
      ,index_table_name :str = None
      ,index_columns    :str = None
      ,ityp_owner       :str = None
      ,ityp_name        :str = None
      ,partitioned      :str = None
      ,iot_type         :str = None
      ,temporary        :str = None
      ,secondary        :str = None
      ,isgeor           :str = None
      ,lob_table_name   :str = None
      ,lob_column_name  :str = None
      ,lob_index_name   :str = None
      ,lob_varray_owner :str = None
      ,lob_varray_name  :str = None
      ,lob_securefile   :str = None
   ):
   
      self._parent_resource  = parent_resource;
      self._parent_secondary = parent_secondary;
      self._sqliteconn       = parent_resource._sqliteconn;
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
          
      self._index_type        = index_type;
      self._index_parameters  = index_parameters;
      self._index_table_owner = index_table_owner;
      self._index_table_name  = index_table_name;
      self._index_columns     = index_columns;
      self._ityp_owner        = ityp_owner;
      self._ityp_name         = ityp_name;
      self._partitioned       = partitioned;
      self._iot_type          = iot_type;
      self._temporary         = temporary;
      self._secondary         = secondary;
      self._isgeor            = isgeor;
      self._lob_table_name    = lob_table_name;
      self._lob_column_name   = lob_column_name;
      self._lob_index_name    = lob_index_name;
      self._lob_varray_owner  = lob_varray_owner;
      self._lob_varray_name   = lob_varray_name;
      self._lob_securefile    = lob_securefile;      
      
      curs = parent_resource._sqliteconn.cursor();
      
      if self._segment_type in ['NESTED TABLE','TABLE','TABLE PARTITION','TABLE SUBPARTITION'] \
      or (self._segment_type is None and self._iot_type == 'IOT'):
         
         # Harvest all table lob segments
         str_sql = """
            SELECT
             a.owner
            ,a.table_name
            ,a.column_name
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
            ,c.type_owner
            ,c.type_name
            ,a.securefile
            FROM
            dba_lobs a
            LEFT JOIN
            segments_compression b
            ON
                a.owner        = b.owner
            AND a.segment_name = b.segment_name
            LEFT JOIN
            dba_varrays c
            ON
                a.owner        = c.owner
            AND a.segment_name = c.lob_name
            WHERE
                a.owner        = :p01
            AND a.table_name   = :p02
         """;

         curs.execute(
             str_sql
            ,{
                'p01':self._owner,'p02':self._segment_name
             }
         );
         
         for row in curs: 
            owner            = row[0];
            lob_table_name   = row[1];
            lob_column_name  = row[2];
            segment_name     = row[3];
            partition_name   = row[4];
            segment_type     = row[5];
            tablespace_name  = row[6];
            compression      = row[7];
            src_compression  = row[8];
            bytes_used       = row[9];
            bytes_comp_none  = row[10];
            bytes_comp_low   = row[11];
            bytes_comp_high  = row[12];
            bytes_comp_unk   = row[13];
            lob_varray_owner = row[14];
            lob_varray_name  = row[15];
            lob_securefile   = row[16];
         
            if (owner,segment_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(owner,segment_name,partition_name)] = Secondary(
                   parent_resource  = parent_resource
                  ,parent_secondary = self
                  ,owner            = owner
                  ,depth            = depth + 1
                  ,segment_name     = segment_name
                  ,partition_name   = partition_name
                  ,segment_type     = segment_type
                  ,tablespace_name  = tablespace_name
                  ,compression      = compression
                  ,src_compression  = src_compression
                  ,bytes_used       = bytes_used
                  ,bytes_comp_none  = bytes_comp_none
                  ,bytes_comp_low   = bytes_comp_low
                  ,bytes_comp_high  = bytes_comp_high
                  ,bytes_comp_unk   = bytes_comp_unk
                  ,secondary        = 'Y'
                  ,lob_table_name   = lob_table_name 
                  ,lob_column_name  = lob_column_name
                  ,lob_varray_owner = lob_varray_owner
                  ,lob_varray_name  = lob_varray_name
                  ,lob_securefile   = lob_securefile
               );
            
         # Harvest all table indexes
         str_sql = """
            SELECT
             a.owner
            ,a.index_name
            ,a.partition_name
            ,a.segment_type
            ,a.tablespace_name
            ,a.compression
            ,a.src_compression
            ,a.bytes_used
            ,CASE WHEN a.compression = 'NONE' THEN a.bytes_used ELSE 0 END AS bytes_comp_none
            ,CASE WHEN a.compression = 'LOW'  THEN a.bytes_used ELSE 0 END AS bytes_comp_low
            ,CASE WHEN a.compression = 'HIGH' THEN a.bytes_used ELSE 0 END AS bytes_comp_high
            ,CASE WHEN a.compression = 'UNK'  THEN a.bytes_used ELSE 0 END AS bytes_comp_unk
            ,a.index_type
            ,a.parameters
            ,a.table_owner
            ,a.table_name
            ,a.ityp_owner
            ,a.ityp_name
            ,a.index_columns
            FROM (
               SELECT
                aa.owner
               ,aa.index_name
               ,bb.partition_name
               ,CASE
                WHEN bb.segment_type IS NULL
                THEN
                  'INDEX'
                ELSE
                  bb.segment_type
                END AS segment_type
               ,aa.tablespace_name
               ,CASE
                WHEN bb.compression IS NULL
                THEN
                  CASE
                  WHEN aa.ityp_owner = 'MDSYS'
                  AND  aa.ityp_name IN ('SPATIAL_INDEX','SPATIAL_INDEX_V2')
                  AND INSTR(UPPER(aa.parameters),'COMPRESSION') > 0
                  AND INSTR(UPPER(aa.parameters),'SECUREFILE') > 0
                  THEN
                     CASE
                     WHEN INSTR(UPPER(aa.parameters),'HIGH') > 0
                     THEN
                        'HIGH'
                     WHEN INSTR(UPPER(aa.parameters),'MEDIUM') > 0
                     THEN
                        'MEDIUM'
                     WHEN INSTR(UPPER(aa.parameters),'LOW') > 0
                     THEN
                        'LOW'
                     ELSE
                        'NONE'                  
                     END
                  ELSE
                     'NONE'
                  END
                ELSE
                  bb.compression
                END as compression
               ,aa.compression AS src_compression
               ,CASE
                WHEN bb.bytes_used IS NULL
                THEN
                  0
                ELSE
                  bb.bytes_used
                END AS bytes_used
               ,aa.index_type
               ,aa.parameters
               ,aa.table_owner
               ,aa.table_name
               ,aa.ityp_owner
               ,aa.ityp_name
               ,aa.index_columns
               FROM
               dba_indexes_plus aa
               LEFT JOIN
               segments_compression bb
               ON
                   aa.owner        = bb.owner
               AND aa.index_name   = bb.segment_name
               WHERE
                   aa.table_owner = :p01
               AND aa.table_name  = :p02
            ) a
         """;

         curs.execute(
             str_sql
            ,{
               'p01':self._owner,'p02':self._segment_name
             }
         );
         
         for row in curs:
            index_owner       = row[0];
            index_name        = row[1];
            partition_name    = row[2];
            segment_type      = row[3];
            tablespace_name   = row[4];
            compression       = row[5];
            src_compression   = row[6];
            bytes_used        = row[7];
            bytes_comp_none   = row[8];
            bytes_comp_low    = row[9];
            bytes_comp_high   = row[10];
            bytes_comp_unk    = row[11];
            index_type        = row[12];
            index_parameters  = row[13];
            index_table_owner = row[14];
            index_table_name  = row[15];
            ityp_owner        = row[16];
            ityp_name         = row[17];
            index_columns     = row[18];
            
            if (index_owner,index_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(index_owner,index_name,partition_name)] = Secondary(
                   parent_resource   = parent_resource
                  ,parent_secondary  = self
                  ,owner             = index_owner
                  ,depth             = depth + 1
                  ,segment_name      = index_name
                  ,partition_name    = partition_name
                  ,segment_type      = segment_type
                  ,tablespace_name   = tablespace_name
                  ,compression       = compression
                  ,src_compression   = src_compression
                  ,bytes_used        = bytes_used
                  ,bytes_comp_none   = bytes_comp_none
                  ,bytes_comp_low    = bytes_comp_low
                  ,bytes_comp_high   = bytes_comp_high
                  ,bytes_comp_unk    = bytes_comp_unk
                  ,index_type        = index_type
                  ,index_parameters  = index_parameters
                  ,index_table_owner = index_table_owner
                  ,index_table_name  = index_table_name
                  ,index_columns     = index_columns
                  ,ityp_owner        = ityp_owner
                  ,ityp_name         = ityp_name
                  ,secondary         = 'Y'
               );
    
      elif self._index_type == 'DOMAIN':
      
         curs2 = parent_resource._sqliteconn.cursor();
               
         if self._ityp_owner == 'CTXSYS' and self._ityp_name == 'CONTEXT':
            str_sql = """
               SELECT
                a.owner
               ,a.segment_name
               ,a.partition_name
               ,a.segment_type
               ,a.tablespace_name
               ,a.compression
               ,a.src_compression
               ,a.src_compress_for
               ,a.bytes_used
               ,CASE WHEN a.compression = 'NONE' THEN a.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN a.compression = 'LOW'  THEN a.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN a.compression = 'HIGH' THEN a.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN a.compression = 'UNK'  THEN a.bytes_used ELSE 0 END AS bytes_comp_unk
               ,a.iot_type
               ,a.temporary
               ,a.isgeor
               FROM
               segments_compression a
               WHERE
                   a.owner         = :p01
               AND a.segment_name LIKE :p02
               AND a.segment_type = 'TABLE'
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
               temporary        = row[14];
               isgeor           = row[15];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource  = parent_resource
                     ,parent_secondary = self
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
                     ,temporary        = temporary
                     ,isgeor           = isgeor
                     ,secondary        = 'Y'
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
               print(
                   '== mdsys spatial index error ==\n' +
                   '   ' + self._owner + '.' + self._segment_name + ' on ' + 
                   self._index_table_owner + '.' + self._index_table_name + '\n'
                  ,file=sys.stderr
               );
            
            else:
               str_sql = """
                  SELECT
                   a.owner
                  ,a.segment_name
                  ,a.partition_name
                  ,a.segment_type
                  ,a.tablespace_name
                  ,a.compression
                  ,a.src_compression
                  ,a.src_compress_for
                  ,a.bytes_used
                  ,CASE WHEN a.compression = 'NONE' THEN a.bytes_used ELSE 0 END AS bytes_comp_none
                  ,CASE WHEN a.compression = 'LOW'  THEN a.bytes_used ELSE 0 END AS bytes_comp_low
                  ,CASE WHEN a.compression = 'HIGH' THEN a.bytes_used ELSE 0 END AS bytes_comp_high
                  ,CASE WHEN a.compression = 'UNK'  THEN a.bytes_used ELSE 0 END AS bytes_comp_unk
                  ,a.iot_type
                  ,a.temporary
                  ,a.isgeor
                  FROM
                  segments_compression a
                  WHERE
                      a.owner        = :p01
                  AND a.segment_name IN (:p02,:p03,:p04)
                  AND a.segment_type = 'TABLE'
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
                  temporary        = row[14];
                  isgeor           = row[15];
                  
                  if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                     parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                         parent_resource  = parent_resource
                        ,parent_secondary = self
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
                        ,temporary        = temporary
                        ,isgeor           = isgeor
                        ,secondary        = 'Y'
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
               ,a.partition_name
               ,a.segment_type
               ,a.tablespace_name
               ,a.compression
               ,a.src_compression
               ,a.src_compress_for
               ,a.bytes_used
               ,CASE WHEN a.compression = 'NONE' THEN a.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN a.compression = 'LOW'  THEN a.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN a.compression = 'HIGH' THEN a.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN a.compression = 'UNK'  THEN a.bytes_used ELSE 0 END AS bytes_comp_unk
               ,a.iot_type
               ,a.temporary
               ,a.isgeor
               FROM
               segments_compression a
               WHERE
                   a.owner        = :p01
               AND a.table_name   = :p02
               AND a.segment_type = 'TABLE'
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
               temporary        = row[14];
               isgeor           = row[15];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource  = parent_resource
                     ,parent_secondary = self
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
                     ,temporary        = temporary
                     ,isgeor           = isgeor
                     ,secondary        = 'Y'
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
                  ,parent_secondary = self
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
                  ,secondary        = 'Y'
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
                  ,parent_secondary = self
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
                  ,secondary        = 'Y'
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
      
   @property
   def index_type(self):
      return self._index_type;      
    
   @property
   def index_parameters(self):
      return self._index_parameters;
      
   @property
   def index_table_owner(self):
      return self._index_table_owner;
      
   @property
   def index_table_name(self):
      return self._index_table_name;
      
   @property
   def index_columns(self):
      return self._index_columns;
      
   @property
   def iot_type(self):
      return self._iot_type;
      
   @property
   def temporary(self):
      return self._temporary;
      
   @property
   def isgeor(self):
      return self._isgeor;
      
   @property
   def secondary(self):
      return self._secondary;
   
   @property
   def ityp_owner(self):
      return self._ityp_owner;

   @property
   def ityp_name(self):
      return self._ityp_name;      
   
   @property
   def lob_table_name(self):
      return self._lob_table_name;

   @property
   def lob_column_name(self):
      return self._lob_column_name;
      
   @property
   def lob_index_name(self):
      return self._lob_index_name;

   @property
   def lob_varray_owner(self):
      return self._lob_varray_owner;

   @property
   def lob_varray_name(self):
      return self._lob_varray_name;
 
   @property
   def lob_securefile(self):
      return self._lob_securefile;
      
   ####
   def bytes_used(
       self
      ,igtbs: list = None
   ) -> float:
      return self._bytes_used;
      
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
      return self._bytes_comp_none;
      
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
      return self._bytes_comp_low;
      
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
      return self._bytes_comp_high;
   
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
      return self._bytes_comp_unk;
      
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
      ,rebuild_indx_flg: bool = False
      ,rebuild_spatial: bool = False
   ) -> list[str]:
      
      if recipe == 'HIGH':

         if self.segment_name in ['JAVA$OPTIONS']:
            # Don't touch system tables
            None;
         
         elif self.isgeor is not None or \
         (self._parent_secondary is not None and self._parent_secondary.isgeor is not None):
            # Pass on all compression for georaster managed items
            rez = [];
            rez.append('/* GeoRaster Table ' + self.owner + '.' + self.lob_table_name + ' */');
            return rez;
      
         elif self.segment_type == 'LOBSEGMENT' and self.compression != 'HIGH' \
         and (                                                            \
               self._parent_secondary is None                             \
            or self._parent_secondary.secondary is None                   \
            or self._parent_secondary.secondary == 'N'                    \
         ):
            rez = Lob(
                parent            = self._parent_resource
               ,owner             = self.owner
               ,table_name        = self.lob_table_name
               ,column_name       = self.lob_column_name
               ,segment_name      = self.segment_name
               ,tablespace_name   = self.tablespace_name
               ,index_name        = self.lob_index_name
               ,securefile        = self.lob_securefile
               ,compression       = self.compression
               ,src_compression   = self.src_compression
               ,varray_type_owner = self.lob_varray_owner
               ,varray_type_name  = self.lob_varray_name
            ).rebuild(
               set_compression = 'HIGH'
            );
            
            return rez;
                       
         elif self.segment_type == 'TABLE'                            \
         and (self.temporary   is None or self.temporary   != 'Y'):
            
            if self.compression != 'HIGH':
               
               if self.iot_type == 'IOT':
                  rez = [];
                  
                  rez.append('/* IOT TABLE ' + self.owner + '.' + self.segment_name + ' */ ');            
                  return rez;
               
               elif self._parent_secondary is not None:
                  if self._parent_secondary.isgeor is not None:
                     None;
                  
                  elif self._parent_secondary.secondary is None \
                  or   self._parent_secondary.secondary == 'N':
                     rez = [];
                     
                     rez.append('ALTER TABLE ' + self.owner + '.' + self.segment_name + ' ' \
                        + 'MOVE COMPRESS FOR OLTP;');            
                     return rez;
                     
               else:
                  rez = [];
                     
                  rez.append('ALTER TABLE ' + self.owner + '.' + self.segment_name + ' ' \
                     + 'MOVE COMPRESS FOR OLTP;');            
                  return rez;
               
         elif self.segment_type == 'INDEX'                                \
         and (                                                            \
               self._parent_secondary is None                             \
            or self._parent_secondary.secondary is None                   \
            or self._parent_secondary.secondary == 'N'                    \
         ):
            
            if self.index_type != 'BITMAP' and self.compression != 'HIGH':
            
               rez = Index(
                   parent           = self
                  ,index_owner      = self.owner
                  ,index_name       = self.segment_name
                  ,index_type       = self.index_type
                  ,table_owner      = self.index_table_owner
                  ,table_name       = self.index_table_name
                  ,index_parameters = self.index_parameters
                  ,ityp_owner       = self.ityp_owner
                  ,ityp_name        = self.ityp_name
                  ,index_columns    = self.index_columns
               ).rebuild(
                   rebuild_spatial = rebuild_spatial
                  ,set_compression = 'HIGH'    
               );
    
               return rez;                                      
               
      elif recipe == 'REBUILDSPX':
         
         if  self.segment_type == 'INDEX' \
         and self.ityp_owner == 'MDSYS' \
         and self.ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
            
            rez = Index(
                parent           = self
               ,index_owner      = self.owner
               ,index_name       = self.segment_name
               ,index_type       = self.index_type
               ,table_owner      = self.index_table_owner
               ,table_name       = self.index_table_name
               ,index_parameters = self.index_parameters
               ,ityp_owner       = self.ityp_owner
               ,ityp_name        = self.ityp_name
               ,index_columns    = self.index_columns
            ).rebuild(
                rebuild_spatial = rebuild_spatial
            );
 
            return rez;

      elif recipe == 'SHRINKSFLOB':
   
         if self.segment_type == 'LOBSEGMENT' and self.lob_securefile == 'YES':
            rez = Lob(
                parent            = self._parent
               ,owner             = self.owner
               ,table_name        = self.lob_table_name
               ,column_name       = self.lob_column_name
               ,segment_name      = self.segment_name
               ,tablespace_name   = self.tablespace_name
               ,index_name        = self.lob_index_name
               ,securefile        = self.lob_securefile
               ,compression       = self.compression
               ,src_compression   = self.src_compression
               ,varray_type_owner = self.lob_varray_owner
               ,varray_type_name  = self.lob_varray_name
            ).rebuild();
            
            return rez; 
      
      if rebuild_indx_flg:
         
         if self.segment_type == 'INDEX':
         
            rez = Index(
                parent           = self
               ,index_owner      = self.owner
               ,index_name       = self.segment_name
               ,index_type       = self.index_type
               ,table_owner      = self.index_table_owner
               ,table_name       = self.index_table_name
               ,index_parameters = self.index_parameters
               ,ityp_owner       = self.ityp_owner
               ,ityp_name        = self.ityp_name
               ,index_columns    = self.index_columns
            ).rebuild(rebuild_spatial=rebuild_spatial);
 
            return rez;
                  
      return None;