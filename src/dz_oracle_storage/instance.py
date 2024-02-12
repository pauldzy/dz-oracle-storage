import os,sys,inspect;
import sqlite3,cx_Oracle;

from .util import slugify;
from .tablespace import Tablespace;
from .datafile import Datafile;
from .tablespacegroup import TablespaceGroup;
from .schema import Schema;
from .schemagroup import SchemaGroup
from .resourcegroup import ResourceGroup;

class Instance(object):

   tsf = 'YYYY-MM-DD HH24:MI:SS.FF6 TZR';
   
   def __init__(
       self
      ,name: str
      ,username: str
      ,password: str
      ,hoststring: str
      ,sqlite_location: str  = None
      ,use_flashback: bool   = False
      ,use_existing_db: bool = False
      ,harvest_extents: bool = False
   ):
   
      self._name            = name;
      self._username        = username;
      self._password        = password;
      self._hoststring      = hoststring;
      self._sqlite_location = sqlite_location;
      self._use_flashback   = use_flashback;
      self._use_existing_db = use_existing_db
      self._harvest_extents = harvest_extents;
      
      self._dts             = None;
      self._dts_s           = None;
      
      self._orcl            = None;
      self._has_spatial     = None;
      self._has_text        = None;
      self._has_sde         = None;      

      dbfile = slugify(self._name) + '.db';
      if self._sqlite_location is not None:
         if not os.path.exists(self._sqlite_location):
            raise Exception('sqlite_location not found.');
         
         self._sqlitepath = self._sqlite_location + os.sep + dbfile;

      else:
         self._sqlitepath  = os.path.dirname(
            os.path.abspath(
               inspect.stack()[-1][1]
            )
         ) + os.sep + dbfile;
         
      if self._use_existing_db and os.path.exists(self._sqlitepath):
         print("== using preexisting information per use_existing_db flag for instance " + self._name + ". ==",file=sys.stderr);
         print("== this flag is only intended for debugging purposes. ==",file=sys.stderr);
         print("",file=sys.stderr);
         self._sqliteconn = sqlite3.connect(self._sqlitepath);
         
      else:
         if self._use_existing_db and not os.path.exists(self._sqlitepath):
            print("== no preexisting information per use_existing_db flag found for instance " + self._name + ". ==",file=sys.stderr);
            print("== will extract and load fresh information from oracle. ==",file=sys.stderr);
            print("== this flag is only intended for debugging purposes. ==",file=sys.stderr);
         
         self.initorcl();
         self.deletesqlite();
         self.initsqlite();     
         self.loadorcl();
      
      self._bytes_allocated   = None;
      self._bytes_used        = None;
      self._bytes_free        = None;
      self._bytes_recyclebin  = None;
      
      self._tablespaces       = {};
      self._tablespace_groups = {};
      self._schemas           = {};
      self._schema_groups     = {};
      self._resource_groups   = {};
      
   @property
   def name(self):
      return self._name;
      
   @property
   def username(self):
      return self._username;
      
   @property
   def password(self):
      return self._password;
 
   @property
   def hoststring(self):
      return self._hoststring;
      
   @property
   def use_flashback(self):
      return self._use_flashback;
      
   @property
   def dts(self):
      return self._dts;
      
   @property
   def dts_asof(self):
      if self._use_flashback:
         return "AS OF TIMESTAMP TO_TIMESTAMP('" + self._dts_s + "','" + self.tsf + "') ";
      else:
         return "";
         
   @property
   def dts_s(self):
      return self._dts_s;
   
   @property
   def has_spatial(self):
      return self._has_spatial;
      
   @property
   def has_text(self):
      return self._has_text;

   @property
   def has_sde(self):
      return self._has_sde;
      
   @property
   def dbpath(self):
      return self._dbpath;
      
   @name.setter
   def name(self,value):
      self._name = value;
      
   @username.setter
   def username(self,value):
      self._username = value;
      
   @password.setter
   def password(self,value):
      self._password = value;
      
   @hoststring.setter
   def hoststring(self,value):
      self._hoststring = value;
   
   ####
   def bytes_allocated(self):
      if self._bytes_allocated is None:
         self.loadinstancetotals();
      return self._bytes_allocated;
      
   ####
   def gb_allocated(self):
      return self.bytes_allocated() / 1024 / 1024 / 1024;
   
   ####
   def bytes_used(self):
      if self._bytes_used is None:
         self.loadinstancetotals();
      return self._bytes_used;
      
   ####
   def gb_used(self):
      return self.bytes_used() / 1024 / 1024 / 1024;

   ####
   def bytes_free(self):
      if self._bytes_free is None:
         self.loadinstancetotals();
      return self._bytes_free;
   
   ####
   def gb_free(self):
      return self.bytes_free() / 1024 / 1024 / 1024;
      
   ####
   def bytes_recyclebin(self):
      if self._bytes_recyclebin is None:
         val = 0;
         for item in self.tablespaces_l:
            val += item.bytes_recyclebin();
         self._bytes_recyclebin = val;
      return self._bytes_recyclebin;
   
   ####
   def gb_recyclebin(self):
      return self.bytes_recyclebin() / 1024 / 1024 / 1024;
      
   @property
   def tablespaces(self): 
      if self._tablespaces is None or self._tablespaces == {}:
         self.loadtablespaces();
      return self._tablespaces;
         
   @property
   def tablespaces_l(self):
      return [d for d in self.tablespaces.values()];
   
   @property
   def tablespaces_keys(self):
      return [d for d in self.tablespaces.keys()];
      
   @property
   def tablespace_groups(self): 
      if self._tablespace_groups is None:
         self._tablespace_groups = {};
      return self._tablespace_groups;
         
   @property
   def tablespace_groups_l(self):
      return [d for d in self.tablespace_groups.values()];
      
   @property
   def schemas(self): 
      if self._schemas is None or self._schemas == {}:
         self.loadschemas();
      return self._schemas;
         
   @property
   def schemas_l(self):
      return [d for d in self.schemas.values()];
      
   @property
   def schema_groups(self): 
      if self._schema_groups is None:
         self._schema_groups = {};
      return self._schema_groups;
         
   @property
   def schema_groups_l(self):
      return [d for d in self.schema_groups.values()];
      
   @property
   def resource_groups(self): 
      if self._resource_groups is None:
         self.resource_groups = {};
      return self._resource_groups;
         
   @property
   def resource_groups_l(self):
      return [d for d in self.resource_groups.values()];
      
   ############################################################################
   def initorcl(self):
   
      try:
         self._orcl = cx_Oracle.connect( 
             user     = self._username
            ,password = self._password
            ,dsn      = self._hoststring 
            ,encoding = "UTF-8"   
         );
         
      except cx_Oracle.DatabaseError as e:
         sys.stderr.write("ERROR, unable to log into Oracle with \n");
         sys.stderr.write("    username: " + str(self._username) + "\n");
         sys.stderr.write("    password: XXXXXXXX\n");
         sys.stderr.write("  hoststring: " + str(self._hoststring) + "\n");
         sys.stderr.write("  oracle msg: " + str(e) + "\n");
         sys.exit(-1);

      curs = self._orcl.cursor();
      
      # First verify user has access to dba_segments
      str_sql = """
         SELECT
         1
         FROM
         dba_segments
      """;
      try:
         curs.execute(str_sql);
      except cx_Oracle.DatabaseError as e:
         error, = e.args;
         sys.stderr.write(str_sql + "\n");
         sys.stderr.write('Error: %s'.format(e));
         raise;
      
      # Check if spatial is present
      str_sql = """
         SELECT
         sdo_version()
         FROM
         dual
      """;
      try:
         curs.execute(str_sql);
         self._has_spatial = True;
      except cx_Oracle.DatabaseError as e:
         self._has_spatial = False;
         
      # Check if text is present
      str_sql = """
         SELECT
         1
         FROM
         ctxsys.ctx_version
      """;
      try:
         curs.execute(str_sql);
         self._has_text = True;
      except cx_Oracle.DatabaseError as e:
         self._has_text = False;
         
      # Check if SDE is present
      str_sql = """
         SELECT
         1
         FROM
         sde.version
      """;
      try:
         curs.execute(str_sql);
         self._has_sde = True;
      except cx_Oracle.DatabaseError as e:
         self._has_sde = False;
      
      # get the timestamp for queries
      str_sql = """
         SELECT
          SYSTIMESTAMP
         ,TO_CHAR(SYSTIMESTAMP,'""" + self.tsf + """')
         FROM
         dual
      """;
      try:
         curs.execute(str_sql);
      except cx_Oracle.DatabaseError as e:
         sys.stderr.write(str_sql + "\n");
         raise;
         
      row = curs.fetchone();
      
      self._dts   = row[0];
      self._dts_s = row[1];

      curs.close();

   ############################################################################
   def deletesqlite(self):
   
      if os.path.exists(self._sqlitepath):
         os.remove(self._sqlitepath);
   
   ############################################################################
   def initsqlite(self):
   
      if os.path.exists(self._sqlitepath):
         raise Exception("sqlite db already exists");
         
      self._sqliteconn = sqlite3.connect(self._sqlitepath);
      
      c = self._sqliteconn.cursor();
      
      c.executescript("""
         
         CREATE TABLE dba_tablespaces(
             tablespace_name  TEXT    NOT NULL
            ,PRIMARY KEY(tablespace_name)
         );
         
         CREATE TABLE dba_users(
             username         TEXT    NOT NULL
            ,PRIMARY KEY(username)
         );

         CREATE TABLE dba_free_space(
             tablespace_name  TEXT    NOT NULL
            ,file_id          INTEGER NOT NULL
            ,block_id         INTEGER NOT NULL
            ,bytes            NUMERIC
            ,blocks           NUMERIC
            ,PRIMARY KEY(file_id,block_id)
         );
         CREATE INDEX dba_free_space_i01 ON dba_free_space(
             tablespace_name
         );
         
         CREATE TABLE dba_data_files(
             file_name        TEXT    NOT NULL
            ,file_id          INTEGER NOT NULL
            ,tablespace_name  TEXT   
            ,bytes            NUMERIC
            ,blocks           NUMERIC
            ,maxbytes         NUMERIC
            ,maxblocks        NUMERIC
            ,status           TEXT
            ,user_bytes       NUMERIC
            ,user_blocks      NUMERIC
            ,extents_hmw      NUMERIC
            ,db_block_size    NUMERIC
            ,PRIMARY KEY(file_id)
         );
         CREATE INDEX dba_data_files_i01 ON dba_data_files(
             tablespace_name
         );
         
         CREATE TABLE dba_recyclebin(
             owner            TEXT    NOT NULL
            ,object_name      TEXT    NOT NULL
            ,original_name    TEXT    NOT NULL
            ,ts_name          TEXT
         );
         CREATE INDEX dba_recyclebin_i01 ON dba_recyclebin(
             owner
            ,object_name
         );
         CREATE INDEX dba_recyclebin_i02 ON dba_recyclebin(
             ts_name
         );
         
         CREATE TABLE dba_segments(
             owner            TEXT    NOT NULL
            ,segment_name     TEXT    NOT NULL
            ,partition_name   TEXT
            ,tablespace_name  TEXT    NOT NULL
            ,segment_type     TEXT    NOT NULL
            ,bytes            NUMERIC NOT NULL
            ,PRIMARY KEY(owner,segment_name,partition_name)
         );
         CREATE INDEX dba_segments_i01 ON dba_segments(
             owner
            ,segment_name
         );
         CREATE INDEX dba_segments_i02 ON dba_segments(
             tablespace_name
         );
         CREATE INDEX dba_segments_i03 ON dba_segments(
             segment_type
         );
         
         CREATE TABLE dba_tables(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,num_rows         INTEGER
            ,tablespace_name  TEXT
            ,compression      TEXT
            ,compress_for     TEXT
            ,partitioned      TEXT
            ,iot_type         TEXT
            ,temporary        TEXT
            ,secondary        TEXT
            ,PRIMARY KEY(owner,table_name)
         );
         CREATE INDEX dba_tables_i01 ON dba_tables(
             tablespace_name
         );
         CREATE INDEX dba_tables_i02 ON dba_tables(
             secondary
         );
         CREATE INDEX dba_tables_i03 ON dba_tables(
             compress_for
         );
         
         CREATE TABLE dba_tab_partitions(
             table_owner      TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,partition_name   TEXT    NOT NULL
            ,tablespace_name  TEXT
            ,compression      TEXT
            ,compress_for     TEXT
            ,PRIMARY KEY(table_owner,table_name,partition_name)
         );
         CREATE INDEX dba_tab_partitions_i01 ON dba_tab_partitions(
             table_owner
            ,table_name
         );
         CREATE INDEX dba_tab_partitions_i02 ON dba_tab_partitions(
             tablespace_name
         );
         CREATE INDEX dba_tab_partitions_i03 ON dba_tab_partitions(
             compress_for
         );
         
         CREATE TABLE dba_tab_columns(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,column_name      TEXT    NOT NULL
            ,data_type        TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name,column_name)
         );
         CREATE INDEX dba_tab_columns_i01 ON dba_tab_columns(
             owner
            ,table_name
         );
         
         /* dba_indexes */
         CREATE TABLE dba_indexes(
             owner            TEXT    NOT NULL
            ,index_name       TEXT    NOT NULL
            ,table_owner      TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,index_type       TEXT    NOT NULL
            ,compression      TEXT
            ,tablespace_name  TEXT
            ,status           TEXT
            ,domidx_status    TEXT
            ,domidx_opstatus  TEXT
            ,ityp_owner       TEXT
            ,ityp_name        TEXT
            ,parameters       TEXT
            ,PRIMARY KEY(owner,index_name)
         );
         CREATE INDEX dba_indexes_i01 ON dba_indexes(
             table_owner
            ,table_name
         );
         CREATE INDEX dba_indexes_i02 ON dba_indexes(
             tablespace_name
         );
         CREATE INDEX dba_indexes_i03 ON dba_indexes(
             compression
         );
         
         CREATE TABLE dba_ind_columns(
             index_owner      TEXT    NOT NULL
            ,index_name       TEXT    NOT NULL
            ,table_owner      TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,column_name      TEXT
            ,column_position  INTEGER NOT NULL
            ,descend          TEXT
            ,PRIMARY KEY(index_owner,index_name,column_name)
         );
         CREATE INDEX dba_ind_columns_i01 ON dba_ind_columns(
             index_owner
            ,index_name
         );
         CREATE INDEX dba_ind_columns_i02 ON dba_ind_columns(
             table_owner
            ,table_name
         );
         
         CREATE TABLE dba_lobs(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,column_name      TEXT    NOT NULL
            ,segment_name     TEXT    NOT NULL
            ,tablespace_name  TEXT
            ,index_name       TEXT    NOT NULL
            ,compression      TEXT
            ,partitioned      TEXT
            ,securefile       TEXT
            ,PRIMARY KEY(owner,table_name,segment_name)
         );
         CREATE INDEX dba_lobs_i01 ON dba_lobs(
             owner
            ,table_name
         );
         CREATE INDEX dba_lobs_i02 ON dba_lobs(
             owner
            ,index_name
         );
         CREATE INDEX dba_lobs_i03 ON dba_lobs(
             tablespace_name
         );
         CREATE INDEX dba_lobs_i04 ON dba_lobs(
             compression
         );
         
         /* dba_varrays */
         CREATE TABLE dba_varrays(
             owner               TEXT    NOT NULL
            ,parent_table_name   TEXT    NOT NULL
            ,parent_table_column TEXT    NOT NULL
            ,type_owner          TEXT    NOT NULL
            ,type_name           TEXT    NOT NULL
            ,lob_name            TEXT
            ,PRIMARY KEY(owner,parent_table_name,parent_table_column)
         );
         CREATE INDEX dba_varrays_i01 ON dba_varrays(
             owner
            ,parent_table_name
         );
         CREATE INDEX dba_varrays_i02 ON dba_varrays(
             owner
            ,lob_name
         );
         CREATE INDEX dba_varrays_i03 ON dba_varrays(
             type_owner
            ,type_name
         );
         
         /* sdo_index_metadata_table */
         CREATE TABLE sdo_index_metadata_table(
             sdo_index_owner  TEXT    NOT NULL
            ,sdo_index_name   TEXT    NOT NULL
            ,sdo_index_table  TEXT    NOT NULL
            ,PRIMARY KEY(sdo_index_owner,sdo_index_name)
         );
         CREATE INDEX sdo_index_metadata_table_i01 ON sdo_index_metadata_table(
             sdo_index_owner
            ,sdo_index_table
         );
         
         CREATE TABLE all_sdo_geor_sysdata(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,column_name      TEXT    NOT NULL
            ,rdt_table_name   TEXT    NOT NULL
            ,PRIMARY KEY(owner,rdt_table_name)
         );
         CREATE INDEX all_sdo_geor_sysdata_i01 ON all_sdo_geor_sysdata(
             owner
            ,table_name
         );
         
         CREATE TABLE ctx_indexes(
             idx_owner        TEXT    NOT NULL
            ,idx_name         TEXT    NOT NULL
            ,idx_table_owner  TEXT    NOT NULL
            ,idx_table        TEXT    NOT NULL
            ,PRIMARY KEY(idx_owner,idx_name)
         );
         CREATE INDEX ctx_indexes_i01 ON ctx_indexes(
             idx_table_owner
            ,idx_table
         );
         
         CREATE TABLE sde_layers(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,layer_id         TEXT    NOT NULL
            ,layer_config     TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name)
         );
         
         CREATE TABLE sde_dbtune(
             parameter_name   TEXT    NOT NULL
            ,keyword          TEXT    NOT NULL
            ,config_string
            ,PRIMARY KEY(parameter_name,keyword)
         );
         
         CREATE TABLE sde_st_geometry_index(
             owner            TEXT    NOT NULL
            ,table_name       TEXT    NOT NULL
            ,index_name       TEXT    NOT NULL
            ,index_id         TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name,index_name)
         );
         CREATE INDEX sde_st_geometry_index_i01 ON sde_st_geometry_index(
             owner
            ,table_name
         );
         CREATE INDEX sde_st_geometry_index_i02 ON sde_st_geometry_index(
             owner
            ,index_name
         );
         
         CREATE TABLE segments_compression(
             owner            TEXT    NOT NULL
            ,segment_name     TEXT    NOT NULL
            ,partition_name   TEXT
            ,segment_type     TEXT    NOT NULL
            ,tablespace_name  TEXT
            ,bytes_used       NUMERIC
            ,src_compression  TEXT
            ,src_compress_for TEXT
            ,compression      TEXT
            ,partitioned      TEXT
            ,iot_type         TEXT
            ,secondary        TEXT
            ,isgeor           TEXT
            ,PRIMARY KEY(owner,segment_name,partition_name)
         );
         CREATE INDEX segments_compression_i01 ON segments_compression(
             owner
            ,segment_name
         );
         CREATE INDEX segments_compression_i02 ON segments_compression(
             segment_type
         );
         CREATE INDEX segments_compression_i03 ON segments_compression(
             tablespace_name
         );
         CREATE INDEX segments_compression_i04 ON segments_compression(
             iot_type
         );
         CREATE INDEX segments_compression_i05 ON segments_compression(
             secondary
         );         
         CREATE INDEX segments_compression_i06 ON segments_compression(
             isgeor
         );

         CREATE VIEW resource_eligible(
             owner
            ,table_name
            ,partition_name
            ,segment_type
            ,tablespace_name
            ,compression
            ,src_compression
            ,src_compress_for
            ,bytes_used
            ,bytes_comp_none
            ,bytes_comp_low
            ,bytes_comp_high
            ,bytes_comp_unk
            ,partitioned
            ,iot_type
            ,temporary
            ,secondary
            ,isgeor
         )
         AS
         SELECT
          a.owner
         ,a.table_name
         ,b.partition_name
         
         ,CASE
          WHEN b.segment_type IS NULL AND a.iot_type = 'IOT'
          THEN
            'TABLE'
          WHEN b.segment_type IS NULL AND a.partitioned = 'YES'
          THEN
            'TABLE'
          ELSE
            b.segment_type
          END AS segment_type
          
         ,a.tablespace_name  
         ,b.compression
         ,b.src_compression
         ,b.src_compress_for
         
         ,b.bytes_used
         ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
         ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
         ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
         ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
         
         ,a.partitioned
         ,a.iot_type
         ,a.temporary
         ,a.secondary
         ,b.isgeor
         FROM 
         dba_tables a
         LEFT JOIN
         segments_compression b
         ON
             a.owner         = b.owner
         AND a.table_name    = b.segment_name
         AND b.partition_name IS NULL
         WHERE
         a.secondary     = 'N';         
      """);
      
      c.close();
      
   ############################################################################
   def loadorcl(self):
      
      toc   = self._sqliteconn.cursor();
      fromc = self._orcl.cursor();
      
      ## dba_tablespaces
      str_to = """
         INSERT INTO dba_tablespaces(
            tablespace_name
         ) VALUES (
            ?
         )
      """;
      
      str_from = """
         SELECT
         a.tablespace_name
         FROM
         dba_tablespaces a
      """;
      str_from += self.dts_asof;

      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_users
      str_to = """
         INSERT INTO dba_users(
            username
         ) VALUES (
            ?
         )
      """;
      
      str_from = """
         SELECT
         a.username
         FROM
         dba_users a
      """;
      str_from += self.dts_asof;

      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);

      ## dba_free_space
      str_to = """
         INSERT INTO dba_free_space(
             tablespace_name
            ,file_id
            ,block_id
            ,bytes
            ,blocks
         ) VALUES (
            ?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.tablespace_name
         ,a.file_id
         ,a.block_id
         ,a.bytes
         ,a.blocks
         FROM
         dba_free_space a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_data_files
      str_to = """
         INSERT INTO dba_data_files(
             file_name
            ,file_id
            ,tablespace_name
            ,bytes
            ,blocks
            ,maxbytes
            ,maxblocks
            ,status
            ,user_bytes
            ,user_blocks
            ,extents_hmw
            ,db_block_size
         ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.file_name
         ,a.file_id
         ,a.tablespace_name
         ,a.bytes
         ,a.blocks
         ,a.maxbytes
         ,a.maxblocks
         ,a.status
         ,a.user_bytes
         ,a.user_blocks
      """;
      
      if self._harvest_extents:
         str_from += """
         ,b.extents_hmw 
         """;
      else:
         str_from += """
         ,NULL
         """;
      
      str_from += """   
         ,(
            SELECT 
            b.value 
            FROM 
            v$parameter b
            WHERE 
            UPPER(b.name) = 'DB_BLOCK_SIZE'
          ) AS db_block_size
         FROM
         dba_data_files a  
      """;
      
      if self._harvest_extents:
         str_from += """
         LEFT JOIN (
            SELECT 
             bb.file_id
            ,MAX(bb.block_id + bb.BLOCKS - 1) AS extents_hmw 
            FROM 
            dba_extents bb
            GROUP BY 
            bb.file_id 
         ) b
         ON 
         a.file_id = b.file_id
         """;
      
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);

      ## dba_recyclebin
      str_to = """
         INSERT INTO dba_recyclebin(
             owner
            ,object_name
            ,original_name
            ,ts_name
         ) VALUES (
            ?,?,?,?
         )
      """;
      
      str_from  = """
         SELECT
          a.owner
         ,a.object_name
         ,a.original_name
         ,a.ts_name
         FROM
         dba_recyclebin a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
    
      ## dba_segments
      str_to = """
         INSERT INTO dba_segments(
             owner
            ,segment_name
            ,partition_name
            ,tablespace_name
            ,segment_type
            ,bytes
         ) VALUES (
            ?,?,?,?,?,?
         )
      """;
      
      str_from  = """
         SELECT
          a.owner
         ,a.segment_name
         ,a.partition_name
         ,a.tablespace_name
         ,a.segment_type
         ,a.bytes
         FROM
         dba_segments a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_tables
      str_to = """
         INSERT INTO dba_tables(
             owner
            ,table_name
            ,num_rows
            ,tablespace_name
            ,compression
            ,compress_for
            ,partitioned
            ,iot_type
            ,temporary
            ,secondary
         ) VALUES (
            ?,?,?,?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.num_rows
         ,a.tablespace_name
         ,a.compression
         ,a.compress_for
         ,a.partitioned
         ,a.iot_type
         ,a.temporary
         ,a.secondary
         FROM
         dba_tables a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_tab_partitions
      str_to = """
         INSERT INTO dba_tab_partitions(
             table_owner
            ,table_name
            ,partition_name
            ,tablespace_name
            ,compression
            ,compress_for
         ) VALUES (
            ?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.table_owner
         ,a.table_name
         ,a.partition_name
         ,a.tablespace_name
         ,a.compression
         ,a.compress_for
         FROM
         dba_tab_partitions a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_tab_columns
      str_to = """
         INSERT INTO dba_tab_columns(
             owner
            ,table_name
            ,column_name
            ,data_type
         ) VALUES (
            ?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.column_name
         ,a.data_type
         FROM
         dba_tab_columns a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);

      ## dba_indexes
      str_to = """
         INSERT INTO dba_indexes(
             owner
            ,index_name
            ,table_owner
            ,table_name
            ,index_type
            ,compression
            ,tablespace_name
            ,status
            ,domidx_status
            ,domidx_opstatus
            ,ityp_owner
            ,ityp_name
            ,parameters
         ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.index_name
         ,a.table_owner
         ,a.table_name
         ,a.index_type
         ,a.compression
         ,a.tablespace_name
         ,a.status
         ,a.domidx_status
         ,a.domidx_opstatus
         ,a.ityp_owner
         ,a.ityp_name
         ,a.parameters
         FROM
         dba_indexes a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_ind_columns
      str_to = """
         INSERT INTO dba_ind_columns(
             index_owner
            ,index_name
            ,table_owner
            ,table_name
            ,column_name
            ,column_position
            ,descend
         ) VALUES (
            ?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.index_owner
         ,a.index_name
         ,a.table_owner
         ,a.table_name
         ,a.column_name
         ,a.column_position
         ,a.descend
         FROM
         dba_ind_columns a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);

      ## dba_lobs
      str_to = """
         INSERT INTO dba_lobs(
             owner
            ,table_name
            ,column_name
            ,segment_name
            ,tablespace_name
            ,index_name
            ,compression
            ,partitioned
            ,securefile
         ) VALUES (
            ?,?,?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.column_name
         ,a.segment_name
         ,a.tablespace_name
         ,a.index_name
         ,a.compression
         ,a.partitioned
         ,a.securefile
         FROM
         dba_lobs a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## dba_varrays
      str_to = """
         INSERT INTO dba_varrays(
             owner
            ,parent_table_name
            ,parent_table_column
            ,type_owner
            ,type_name
            ,lob_name
         ) VALUES (
            ?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.parent_table_name
         ,a.parent_table_column
         ,a.type_owner
         ,a.type_name
         ,a.lob_name
         FROM
         dba_varrays a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
      ## Spatial
      if self._has_spatial:
         ## sdo_index_metadata_table
         str_to = """
            INSERT INTO sdo_index_metadata_table(
                sdo_index_owner
               ,sdo_index_name
               ,sdo_index_table
            ) VALUES (
               ?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.sdo_index_owner
            ,a.sdo_index_name
            ,a.sdo_index_table
            FROM
            mdsys.sdo_index_metadata_table a
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);

         ## all_sdo_geor_sysdata
         str_to = """
            INSERT INTO all_sdo_geor_sysdata(
                owner
               ,table_name
               ,column_name
               ,rdt_table_name
            ) VALUES (
               ?,?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.owner
            ,a.table_name
            ,a.column_name
            ,a.rdt_table_name
            FROM
            all_sdo_geor_sysdata a
            GROUP BY
             a.owner
            ,a.table_name
            ,a.column_name
            ,a.rdt_table_name
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);
            
      if self._has_text:
         ## ctx_indexes
         str_to = """
            INSERT INTO ctx_indexes(
                idx_owner
               ,idx_name
               ,idx_table_owner
               ,idx_table
            ) VALUES (
               ?,?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.idx_owner
            ,a.idx_name
            ,a.idx_table_owner
            ,a.idx_table
            FROM
            ctxsys.ctx_indexes a
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);
            
      if self._has_sde:
         ## sde_layers
         str_to = """
            INSERT INTO sde_layers(
                owner
               ,table_name
               ,layer_id
               ,layer_config
            ) VALUES (
               ?,?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.owner
            ,a.table_name
            ,a.layer_id
            ,a.layer_config
            FROM
            sde.layers a
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);

         ## sde_dbtune
         str_to = """
            INSERT INTO sde_dbtune(
                parameter_name
               ,keyword
               ,config_string
            ) VALUES (
               ?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.parameter_name
            ,a.keyword
            ,TO_CHAR(SUBSTR(a.config_string,1,4000))
            FROM
            sde.dbtune a
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);            

         ## sde_st_geometry_columns
         str_to = """
            INSERT INTO sde_st_geometry_index(
                owner
               ,table_name
               ,index_name
               ,index_id
            ) VALUES (
               ?,?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.owner
            ,a.table_name
            ,a.index_name
            ,a.index_id
            FROM
            sde.st_geometry_index a
         """;
         str_from += self.dts_asof;
         
         fromc.execute(str_from);
         for row in fromc:
            toc.execute(str_to,row);
          
      # Close the Oracle cursor
      fromc.close();
      
      ## segments_compression
      str_to = """
         INSERT INTO segments_compression(
             owner
            ,segment_name
            ,partition_name
            ,segment_type
            ,tablespace_name
            
            ,compression
            ,src_compression
            ,src_compress_for
            
            ,bytes_used
            
            ,partitioned
            ,iot_type
            ,secondary
            ,isgeor
         )
         SELECT
          bbb.owner
         ,bbb.segment_name
         ,bbb.partition_name
         ,bbb.segment_type
         ,bbb.tablespace_name
         
         /* Compression Logic */
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            CASE 
            WHEN ccc.compress_for IS NULL
            THEN
               'NONE'
            WHEN ccc.compress_for IN ('BASIC')
            THEN
               'LOW'
            WHEN ccc.compress_for IN ('ADVANCED')
            THEN
               'HIGH'
            ELSE
               'UNK'
            END
          WHEN bbb.segment_type IN ('INDEX','LOBINDEX')
          THEN
            CASE 
            WHEN ddd.compression IS NULL
            THEN
               'NONE'
            WHEN ddd.compression = 'DISABLED'
            THEN
               'NONE'
            WHEN ddd.compression IN ('ENABLED')
            THEN
               'LOW'
            WHEN ddd.compression IN ('ADVANCED HIGH')
            THEN
               'HIGH'
            ELSE
               'UNK'
            END
          WHEN bbb.segment_type = 'PARTITION'
          THEN
            CASE 
            WHEN eee.compress_for IS NULL
            THEN
               'NONE'
            WHEN eee.compress_for IN ('BASIC')
            THEN
               'LOW'
            WHEN eee.compress_for IN ('ADVANCED')
            THEN
               'HIGH'
            ELSE
               'UNK'
            END
          WHEN bbb.segment_type = 'LOBSEGMENT'
          THEN
            CASE 
            WHEN hhh.compression IS NULL OR hhh.compression IN ('NONE','NO')
            THEN
               'NONE'
            WHEN hhh.compression IN ('LOW')
            THEN
               'LOW'
            WHEN hhh.compression IN ('HIGH')
            THEN
               'HIGH'
            ELSE
               'UNK'
            END
          ELSE
            'NONE'
          END AS compression
         
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.compression
          WHEN bbb.segment_type IN ('INDEX','LOBINDEX')
          THEN
            ddd.compression
          WHEN bbb.segment_type = 'PARTITION'
          THEN
            eee.compression
          WHEN bbb.segment_type = 'LOBSEGMENT'
          THEN
            hhh.compression
          ELSE
            NULL
          END AS src_compression
          
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.compress_for
          WHEN bbb.segment_type = 'PARTITION'
          THEN
            eee.compress_for
          ELSE
            NULL
          END AS src_compress_for

         ,bbb.bytes AS bytes_used
         
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.partitioned
          ELSE
            NULL
          END AS partitioned
          
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.iot_type
          ELSE
            NULL
          END AS iot_type
          
         ,CASE
          WHEN bbb.segment_type = 'TABLE' AND ggg.rdt_table_name IS NOT NULL
          THEN
            'Y'
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.secondary
          ELSE
            NULL
          END AS secondary
          
         ,CASE 
          WHEN fff.geor_table_name IS NOT NULL
          THEN
            'GEOR'
          WHEN ggg.rdt_table_name IS NOT NULL
          THEN
            'RDT'
          ELSE
            NULL
          END AS isgeor
          
         FROM
         dba_segments bbb
         LEFT JOIN
         dba_tables ccc
         ON
             bbb.owner          = ccc.owner
         AND bbb.segment_name   = ccc.table_name
         LEFT JOIN
         dba_indexes ddd
         ON
             bbb.owner          = ddd.owner
         AND bbb.segment_name   = ddd.index_name
         LEFT JOIN
         dba_tab_partitions eee
         ON
             bbb.owner          = eee.table_owner
         AND bbb.segment_name   = eee.table_name
         AND bbb.partition_name = eee.partition_name
         LEFT JOIN (
            SELECT
             ffff.owner      AS geor_table_owner
            ,ffff.table_name AS geor_table_name
            FROM
            all_sdo_geor_sysdata ffff
            GROUP BY
             ffff.owner
            ,ffff.table_name
         ) fff
         ON
             ccc.owner          = fff.geor_table_owner
         AND ccc.table_name     = fff.geor_table_name
         LEFT JOIN
         all_sdo_geor_sysdata ggg
         ON
             ccc.owner          = ggg.owner
         AND ccc.table_name     = ggg.rdt_table_name
         LEFT JOIN
         dba_lobs hhh
         ON
             bbb.owner          = hhh.owner
         AND bbb.segment_name   = hhh.segment_name;
      """;
      
      toc.execute(str_to);
      
      self._sqliteconn.commit();
      toc.execute("PRAGMA analysis_limit=400;");
      toc.execute("PRAGMA optimize;");
      
      toc.close();
      
   ############################################################################
   def loadinstancetotals(self):
   
      curs = self._sqliteconn.cursor();
      
      str_sql = """
         SELECT
          SUM(a.bytes_alloc)                                           AS bytes_allocated
         ,SUM(a.bytes_alloc - a.bytes_free)                            AS bytes_used
         ,SUM(a.bytes_alloc - (a.bytes_alloc - a.bytes_free))          AS bytes_free
         FROM (  
            SELECT
             CASE 
             WHEN bb.tablespace_name IS NULL
             THEN
               CASE
               WHEN aa.tablespace_name IS NULL
               THEN
                  'UNKNOWN'
               ELSE
                  aa.tablespace_name
               END
             ELSE
               bb.tablespace_name
             END AS tablespace_name
            ,CASE
             WHEN aa.bytes_free IS NULL
             THEN
               0
             ELSE
               aa.bytes_free
             END AS bytes_free
            ,bb.bytes_alloc
            ,bb.data_files
            FROM (
               SELECT
                aaa.tablespace_name
               ,SUM(aaa.bytes) AS bytes_free
               ,MAX(aaa.bytes) AS bytes_largest
               FROM
               dba_free_space aaa
               GROUP BY
               aaa.tablespace_name
            ) aa
            RIGHT JOIN (
               SELECT
                bbb.tablespace_name
               ,SUM(bbb.user_bytes) AS bytes_alloc
               ,COUNT(*) AS data_files
               FROM
               dba_data_files bbb
               GROUP BY
               bbb.tablespace_name
            ) bb
            ON
            aa.tablespace_name = bb.tablespace_name
         ) a
      """;
   
      curs.execute(str_sql);
      for row in curs:
         self._bytes_allocated = row[0];
         self._bytes_used      = row[1];
         self._bytes_free      = row[2];
         
      curs.close();
            
   ############################################################################
   def loadtablespaces(self):
   
      curs = self._sqliteconn.cursor();
   
      str_sql = """
         SELECT
          a.tablespace_name
         ,a.bytes_alloc                                           AS bytes_allocated
         ,a.bytes_alloc - a.bytes_free                            AS bytes_used
         ,a.bytes_alloc - (a.bytes_alloc - a.bytes_free)          AS bytes_free
         ,ROUND((a.bytes_alloc - a.bytes_free) / a.bytes_alloc,5) AS bytes_free_perc
         FROM (  
            SELECT
             CASE 
             WHEN bb.tablespace_name IS NULL
             THEN
               CASE
               WHEN aa.tablespace_name IS NULL
               THEN
                  'UNKNOWN'
               ELSE
                  aa.tablespace_name
               END
             ELSE
               bb.tablespace_name
             END AS tablespace_name
            ,CASE
             WHEN aa.bytes_free IS NULL
             THEN
               0
             ELSE
               aa.bytes_free
             END AS bytes_free
            ,bb.bytes_alloc
            ,bb.data_files
            FROM (
               SELECT
                aaa.tablespace_name
               ,SUM(aaa.bytes) AS bytes_free
               ,MAX(aaa.bytes) AS bytes_largest
               FROM
               dba_free_space aaa
               GROUP BY
               aaa.tablespace_name
            ) aa
            RIGHT JOIN (
               SELECT
                bbb.tablespace_name
               ,SUM(bbb.user_bytes) AS bytes_alloc
               ,COUNT(*) AS data_files
               FROM
               dba_data_files bbb
               GROUP BY
               bbb.tablespace_name
            ) bb
            ON
            aa.tablespace_name = bb.tablespace_name
         ) a
      """;
      
      self._tablespaces = {};
      
      curs.execute(str_sql);
      for row in curs:
         tablespace_name = row[0];
         bytes_allocated = row[1];
         bytes_used      = row[2];
         bytes_free      = row[3];
      
         self._tablespaces[tablespace_name] = Tablespace(
             parent          = self
            ,tablespace_name = tablespace_name
            ,bytes_allocated = bytes_allocated
            ,bytes_used      = bytes_used
            ,bytes_free      = bytes_free
         );
         
      str_sql = """
         SELECT
          a.ts_name    AS tablespace_name
         ,SUM(b.bytes) AS bytes_recyclebin
         FROM (
            SELECT
             aa.owner
            ,aa.object_name
            ,aa.ts_name
            FROM
            dba_recyclebin aa
            WHERE
            aa.ts_name IS NOT NULL
            GROUP BY
             aa.owner
            ,aa.object_name
            ,aa.ts_name
         ) a
         JOIN
         dba_segments b
         ON
             a.owner       = b.owner
         AND a.object_name = b.segment_name
         GROUP BY
         a.ts_name
      """;
      
      curs.execute(str_sql);
      for row in curs:
         tablespace_name  = row[0];
         bytes_recyclebin = row[1];
         
         self._tablespaces[tablespace_name]._bytes_recyclebin = bytes_recyclebin;
         
      curs.close();
      
   ############################################################################
   def loadschemas(self):
   
      curs = self._sqliteconn.cursor();
   
      str_sql = """
         SELECT
          a.username
          
         ,CASE
          WHEN b.bytes_used IS NULL
          THEN
            0
          ELSE
            b.bytes_used
          END AS bytes_used
          
         ,CASE
          WHEN b.bytes_comp_none IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_none
          END AS bytes_comp_none
          
         ,CASE
          WHEN b.bytes_comp_low IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_low
          END AS bytes_comp_low
          
         ,CASE
          WHEN b.bytes_comp_high IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_high
          END AS bytes_comp_high
          
         ,CASE
          WHEN b.bytes_comp_unk IS NULL
          THEN
            0
          ELSE
            b.bytes_comp_unk
          END AS bytes_comp_unk
          
         FROM 
         dba_users a
         LEFT JOIN (
            SELECT
             bb.owner
            ,SUM(bb.bytes_used)      AS bytes_used
            ,SUM(bb.bytes_comp_none) AS bytes_comp_none
            ,SUM(bb.bytes_comp_low)  AS bytes_comp_low
            ,SUM(bb.bytes_comp_high) AS bytes_comp_high
            ,SUM(bb.bytes_comp_unk)  AS bytes_comp_unk
            FROM (
               SELECT
                bbb.owner
               ,bbb.bytes_used
               ,CASE WHEN bbb.compression = 'NONE' THEN bbb.bytes_used ELSE 0 END AS bytes_comp_none
               ,CASE WHEN bbb.compression = 'LOW'  THEN bbb.bytes_used ELSE 0 END AS bytes_comp_low
               ,CASE WHEN bbb.compression = 'HIGH' THEN bbb.bytes_used ELSE 0 END AS bytes_comp_high
               ,CASE WHEN bbb.compression = 'UNK'  THEN bbb.bytes_used ELSE 0 END AS bytes_comp_unk
               FROM
               segments_compression bbb
            ) bb
            GROUP BY
            bb.owner
         ) b
         ON
         a.username = b.owner
      """;
      
      self._schemas = {};
      curs.execute(str_sql);
      for row in curs:
         schema_name     = row[0];
         bytes_used      = row[1];
         bytes_comp_none = row[2];
         bytes_comp_low  = row[3];
         bytes_comp_high = row[4];
         bytes_comp_unk  = row[5];
      
         self._schemas[schema_name] = Schema(
             self
            ,schema_name     = schema_name
            ,bytes_used      = bytes_used
            ,bytes_comp_none = bytes_comp_none
            ,bytes_comp_low  = bytes_comp_low
            ,bytes_comp_high = bytes_comp_high
            ,bytes_comp_unk  = bytes_comp_unk
         );
         
      curs.close();
      
   ############################################################################
   def add_tablespace_group(
       self
      ,tablespace_group_name: str
   ):
   
      if self._tablespace_groups is None:
         self._tablespace_groups = {};
         
      self._tablespace_groups[tablespace_group_name] = TablespaceGroup(
          parent                = self
         ,tablespace_group_name = tablespace_group_name
      );
      
   ############################################################################
   def delete_tablespace_group(
       self
      ,tablespace_group_name: str
   ):
   
      if self._tablespace_groups is None:
         self._tablespace_groups = {};

      if tablespace_group_name in self._tablespace_groups:
         del self._tablespace_groups[tablespace_group_name];
      
   ############################################################################
   def add_schema_group(
       self
      ,schema_group_name: str
      ,ignore_tablespaces: bool = None
   ):
   
      if self._schema_groups is None:
         self._schema_groups = {};
         
      self._schema_groups[schema_group_name] = SchemaGroup(
          parent              = self
         ,schema_group_name   = schema_group_name
      );
      
      if ignore_tablespaces is not None:
         self._schema_groups[schema_group_name].set_ignore_tablespaces(
            tablespace_names = ignore_tablespaces
         );
      
   ############################################################################
   def delete_schema_group(
       self
      ,schema_group_name: str
   ):
   
      if self._schema_groups is None:
         self._schema_groups = {};

      if schema_group_name in self._schema_groups:
         del self._schema_groups[schema_group_name];
      
   ############################################################################
   def add_resource_group(
       self
      ,resource_group_name: str
   ):
   
      if self._resource_groups is None:
         self._resource_groups = {};
         
      self._resource_groups[resource_group_name] = ResourceGroup(
          parent              = self
         ,resource_group_name = resource_group_name
      );
      
   ############################################################################
   def delete_resource_group(
       self
      ,resource_group_name: str
   ):
   
      if self._resource_groups is None:
         self._resource_groups = {};

      if resource_group_name in self._resource_groups:
         del self._resource_groups[resource_group_name];
         
