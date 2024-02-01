import os,sys,inspect;
import sqlite3,cx_Oracle;
from datetime import datetime;
from .util import slugify;

class Instance(object):

   tsf = 'YYYY-MM-DD HH24:MI:SS.FF6 TZR';
   
   def __init__(
       self
      ,name
      ,username
      ,password
      ,hoststring
      ,sqlite_location = None
      ,use_flashback   = False
      ,use_existing_db = False
   ):
   
      self._name            = name;
      self._username        = username;
      self._password        = password;
      self._hoststring      = hoststring;
      self._sqlite_location = sqlite_location;
      self._use_flashback   = use_flashback;
      self._use_existing_db = use_existing_db
      
      self._dts             = None;
      self._dts_s           = None;
      
      self._orcl            = None;
      self._has_spatial     = None;
      self._has_text        = None;
      self._has_sde         = None;      

      dbfile = slugify(name) + '.db';
      if self._sqlite_location is not None:
         if not os.path.exists(self._sqlite_location):
            raise Exception('sqlite_location not found.');
         
         self._sqlitepath = self._sqlite_location + os.sep + dbfile;

      else:
         self._sqlitepath  = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0))) + os.sep + dbfile;
         
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
   
   @property
   def bytes_allocated(self):
      if self._bytes_allocated is None:
         self.loadinstancetotals();
      return self._bytes_allocated;
      
   @property
   def gb_allocated(self):
      return self.bytes_allocated / 1024 / 1024 / 1024;
   
   @property
   def bytes_used(self):
      if self._bytes_used is None:
         self.loadinstancetotals();
      return self._bytes_used;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;

   @property
   def bytes_free(self):
      if self._bytes_free is None:
         self.loadinstancetotals();
      return self._bytes_free;
   
   @property
   def gb_free(self):
      return self.bytes_free / 1024 / 1024 / 1024;
      
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
             tablespace_name TEXT    NOT NULL
            ,PRIMARY KEY(tablespace_name)
         );
         
         CREATE TABLE dba_users(
             username        TEXT    NOT NULL
            ,PRIMARY KEY(username)
         );

         CREATE TABLE dba_free_space(
             tablespace_name TEXT    NOT NULL
            ,file_id         INTEGER NOT NULL
            ,block_id        INTEGER NOT NULL
            ,bytes           NUMERIC NOT NULL
            ,PRIMARY KEY(tablespace_name,file_id,block_id)
         );
         
         CREATE TABLE dba_data_files(
             tablespace_name TEXT    NOT NULL
            ,file_id         INTEGER NOT NULL
            ,user_bytes      NUMERIC NOT NULL
            ,PRIMARY KEY(tablespace_name,file_id)
         );
         
         CREATE TABLE dba_segments(
             owner           TEXT    NOT NULL
            ,segment_name    TEXT    NOT NULL
            ,partition_name  TEXT
            ,tablespace_name TEXT    NOT NULL
            ,segment_type    TEXT    NOT NULL
            ,bytes           NUMERIC NOT NULL
            ,PRIMARY KEY(owner,segment_name,partition_name)
         );
         
         CREATE TABLE dba_tables(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,num_rows        INTEGER
            ,tablespace_name TEXT
            ,compress_for    TEXT
            ,partitioned     TEXT
            ,iot_type        TEXT
            ,secondary       TEXT
            ,PRIMARY KEY(owner,table_name)
         );
         
         CREATE TABLE dba_tab_partitions(
             table_owner     TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,partition_name  TEXT    NOT NULL
            ,tablespace_name TEXT
            ,compress_for    TEXT
            ,PRIMARY KEY(table_owner,table_name,partition_name)
         );
         
         CREATE TABLE dba_tab_columns(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,column_name     TEXT    NOT NULL
            ,data_type       TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name,column_name)
         );
         
         CREATE TABLE dba_indexes(
             owner           TEXT    NOT NULL
            ,index_name      TEXT    NOT NULL
            ,table_owner     TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,index_type      TEXT    NOT NULL
            ,compression     TEXT
            ,status          TEXT
            ,domidx_status   TEXT
            ,domidx_opstatus TEXT
            ,ityp_owner      TEXT
            ,ityp_name       TEXT
            ,PRIMARY KEY(owner,index_name)
         );
         
         CREATE TABLE dba_ind_columns(
             index_owner     TEXT    NOT NULL
            ,index_name      TEXT    NOT NULL
            ,column_name     TEXT    NOT NULL
            ,PRIMARY KEY(index_owner,index_name,column_name)
         );
         
         CREATE TABLE dba_lobs(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,segment_name    TEXT    NOT NULL
            ,index_name      TEXT    NOT NULL
            ,compression     TEXT
            ,PRIMARY KEY(owner,table_name,segment_name)
         );
         
         CREATE TABLE sdo_index_metadata_table(
             sdo_index_owner TEXT    NOT NULL
            ,sdo_index_name  TEXT    NOT NULL
            ,sdo_index_table TEXT    NOT NULL
            ,PRIMARY KEY(sdo_index_owner,sdo_index_name)
         );
         
         CREATE TABLE all_sdo_geor_sysdata(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,column_name     TEXT    NOT NULL
            ,rdt_table_name  TEXT    NOT NULL
            ,PRIMARY KEY(owner,rdt_table_name)
         );
         
         CREATE TABLE ctx_indexes(
             idx_owner       TEXT    NOT NULL
            ,idx_name        TEXT    NOT NULL
            ,idx_table_owner TEXT    NOT NULL
            ,idx_table       TEXT    NOT NULL
            ,PRIMARY KEY(idx_owner,idx_name)
         );
         
         CREATE TABLE sde_layers(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,layer_id        TEXT    NOT NULL
            ,layer_config    TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name)
         );
         
         CREATE TABLE sde_dbtune(
             parameter_name  TEXT    NOT NULL
            ,keyword         TEXT    NOT NULL
            ,config_string
            ,PRIMARY KEY(parameter_name,keyword)
         );
         
         CREATE TABLE sde_st_geometry_index(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,index_name      TEXT    NOT NULL
            ,index_id        TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name,index_name)
         );
         
         CREATE VIEW segments_compression(
             owner
            ,segment_name
            ,partition_name
            ,segment_type
            ,tablespace_name
            ,bytes_used
            ,compression
            ,partitioned
            ,iot_type
            ,secondary
            ,isgeor
         )
         AS
         SELECT
          bbb.owner
         ,bbb.segment_name
         ,bbb.partition_name
         ,bbb.segment_type
         ,bbb.tablespace_name
         ,bbb.bytes AS bytes_used
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
          WHEN bbb.segment_type = 'INDEX'
          THEN
            CASE 
            WHEN ddd.compression IS NULL OR ddd.compression = 'DISABLED'
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
          ELSE
            'NONE'
          END AS compression
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
         AND ccc.table_name     = ggg.rdt_table_name;    
      """);
      
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
         ) VALUES (
            ?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.tablespace_name
         ,a.file_id
         ,a.block_id
         ,a.bytes
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
             tablespace_name
            ,file_id
            ,user_bytes
         ) VALUES (
            ?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.tablespace_name
         ,a.file_id
         ,a.user_bytes
         FROM
         dba_data_files a
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
            ,compress_for
            ,partitioned
            ,iot_type
            ,secondary
         ) VALUES (
            ?,?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.num_rows
         ,a.tablespace_name
         ,a.compress_for
         ,a.partitioned
         ,a.iot_type
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
            ,compress_for
         ) VALUES (
            ?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.table_owner
         ,a.table_name
         ,a.partition_name
         ,a.tablespace_name
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
            ,status
            ,domidx_status
            ,domidx_opstatus
            ,ityp_owner
            ,ityp_name
         ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?
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
         ,a.status
         ,a.domidx_status
         ,a.domidx_opstatus
         ,a.ityp_owner
         ,a.ityp_name
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
            ,column_name
         ) VALUES (
            ?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.index_owner
         ,a.index_name
         ,a.column_name
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
            ,segment_name
            ,index_name
            ,compression
         ) VALUES (
            ?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.segment_name
         ,a.index_name
         ,a.compression
         FROM
         dba_lobs a
      """;
      str_from += self.dts_asof;
      
      fromc.execute(str_from);
      for row in fromc:
         toc.execute(str_to,row);
         
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
            
      fromc.close();
      self._sqliteconn.commit();
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
            ,SUM(bb.bytes_used) AS bytes_used
            ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
            ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
            ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
            ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
            FROM
            segments_compression bb
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
      ,tablespace_group_name
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
      ,tablespace_group_name
   ):
   
      if self._tablespace_groups is None:
         self._tablespace_groups = {};

      if tablespace_group_name in self._tablespace_groups:
         del self._tablespace_groups[tablespace_group_name];
      
   ############################################################################
   def add_schema_group(
       self
      ,schema_group_name
   ):
   
      if self._schema_groups is None:
         self._schema_groups = {};
         
      self._schema_groups[schema_group_name] = SchemaGroup(
          parent              = self
         ,schema_group_name   = schema_group_name
      );
      
   ############################################################################
   def delete_schema_group(
       self
      ,schema_group_name
   ):
   
      if self._schema_groups is None:
         self._schema_groups = {};

      if schema_group_name in self._schema_groups:
         del self._schema_groups[schema_group_name];
      
   ############################################################################
   def add_resource_group(
       self
      ,resource_group_name
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
      ,resource_group_name
   ):
   
      if self._resource_groups is None:
         self._resource_groups = {};

      if resource_group_name in self._resource_groups:
         del self._resource_groups[resource_group_name];
         
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

   @property
   def bytes_allocated(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_allocated;
      return rez;
      
   @property
   def gb_allocated(self):
      return self.bytes_allocated / 1024 / 1024 / 1024;
   
   @property
   def bytes_used(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_used;
      return rez;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;

   @property
   def bytes_free(self):
      rez = 0;
      for item in self.tablespaces.values():
         rez += item.bytes_free;
      return rez;
   
   @property
   def gb_free(self):
      return self.bytes_free / 1024 / 1024 / 1024;
      
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
         
############################################################################### 
class Tablespace(object):

   def __init__(
       self
      ,parent
      ,tablespace_name
      ,bytes_allocated
      ,bytes_used
      ,bytes_free
   ):
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._tablespace_name = tablespace_name;
      self._bytes_allocated = bytes_allocated;
      self._bytes_used      = bytes_used;
      self._bytes_free      = bytes_free;
      
      self._bytes_comp_none = None;
      self._bytes_comp_low  = None;
      self._bytes_comp_high = None;
      self._bytes_comp_unk  = None;
      
   @property
   def tablespace_name(self):
      return self._tablespace_name;
      
   @property
   def bytes_allocated(self):
      return self._bytes_allocated;
      
   @property
   def gb_allocated(self):
      return self.bytes_allocated / 1024 / 1024 / 1024;
   
   @property
   def bytes_used(self):
      return self._bytes_used;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;

   @property
   def bytes_free(self):
      return self._bytes_free;
   
   @property
   def gb_free(self):
      return self.bytes_free / 1024 / 1024 / 1024;
      
   
   @property
   def bytes_comp_none(self):
      if self._bytes_comp_none is None:
         self.get_segment_size();
      return self._bytes_comp_none;
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      if self._bytes_comp_low is None:
         self.get_segment_size();
      return self._bytes_comp_low;
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      if self._bytes_comp_high is None:
         self.get_segment_size();
      return self._bytes_comp_high;
      
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_unk(self):
      if self._bytes_comp_unk is None:
         self.get_segment_size();
      return self._bytes_comp_unk;
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
      
   ############################################################################
   def get_segment_size(
      self
   ):
      curs = self._sqliteconn.cursor();
      
      str_sql = """
         SELECT
          a.tablespace_name
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
         dba_tablespaces a
         LEFT JOIN (
            SELECT
             bb.tablespace_name
            ,SUM(bb.bytes_used) AS bytes_used
            ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
            ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
            ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
            ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
            FROM
            segments_compression bb
            GROUP BY
            bb.tablespace_name
         ) b
         ON
         a.tablespace_name = b.tablespace_name
         WHERE
         a.tablespace_name = :p01
      """;
         
      curs.execute(
          str_sql
         ,{'p01':self._tablespace_name}    
      );
      for row in curs:
         tablespace_name = row[0];
         bytes_used      = row[1];
         bytes_comp_none = row[2];
         bytes_comp_low  = row[3];
         bytes_comp_high = row[4];
         bytes_comp_unk  = row[5];
      
      self._bytes_comp_none = bytes_comp_none
      self._bytes_comp_low  = bytes_comp_low
      self._bytes_comp_high = bytes_comp_high
      self._bytes_comp_unk  = bytes_comp_unk
            
      curs.close();
     
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
      
   @property
   def schema_group_name(self):
      return self._schema_group_name;
      
   @property
   def schemas(self):
      return self._schemas;
      
   @property
   def schemas_l(self):
      return [d for d in self.schemas.values()];
      
   @property
   def bytes_used(self):
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_used;
      return rez;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_none(self):
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_none;
      return rez;
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_low;
      return rez;
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_high;
      return rez;
      
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_unk(self):
      rez = 0;
      for item in self.schemas.values():
         rez += item.bytes_comp_unk;
      return rez;
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
      
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
      
############################################################################### 
class Schema(object):

   def __init__(
       self
      ,parent
      ,schema_name
      ,bytes_used
      ,bytes_comp_none
      ,bytes_comp_low
      ,bytes_comp_high
      ,bytes_comp_unk
   ):
   
      self._parent             = parent;
      self._sqliteconn         = parent._sqliteconn;
      self._schema_name        = schema_name;
      self._bytes_used         = bytes_used;
      self._bytes_comp_none    = bytes_comp_none;
      self._bytes_comp_low     = bytes_comp_low;
      self._bytes_comp_high    = bytes_comp_high;
      self._bytes_comp_unk     = bytes_comp_unk;
      
      self._ignore_tablespaces = {};
      
   @property
   def schema_name(self):
      return self._schema_name;
      
   @property
   def bytes_used(self):
      return float(self._bytes_used);
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_none(self):
      return float(self._bytes_comp_none);
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      return float(self._bytes_comp_low);
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      return float(self._bytes_comp_high);
      
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_unk(self):
      return float(self._bytes_comp_unk);
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
      
   @property
   def ignore_tablespaces(self):
      return self._ignore_tablespaces;
      
   @property
   def ignore_tablespaces_l(self):
      return [d for d in self.ignore_tablespaces.values()];
      
   ############################################################################
   def add_ignore_tablespace(
       self
      ,tablespace_name
   ):
   
      if self._ignore_tablespaces is None:
         self._ignore_tablespaces = {};
         
      self._ignore_tablespaces[tablespace_name] = tablespace_name;
      self.resample_size();
      
   ############################################################################
   def delete_ignore_tablespace(
       self
      ,tablespace_name
   ):
   
      if self._ignore_tablespaces is None:
         self._ignore_tablespaces = {};

      if tablespace_name in self._ignore_tablespaces:
         del self._ignore_tablespaces[tablespace_name];
         self.resample_size();
         
   ############################################################################
   def resample_size(
      self
   ):
      curs = self._sqliteconn.cursor();
      
      if len(self.ignore_tablespaces_l) > 0:
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
               ,SUM(bb.bytes_used) AS bytes_used
               ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
               ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
               ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
               ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
               FROM
               segments_compression bb
               WHERE
               bb.tablespace_name NOT IN (""" + ",".join(f'\'{w}\'' for w in self.ignore_tablespaces_l) + """)
               GROUP BY
               bb.owner
            ) b
            ON
            a.username = b.owner
            WHERE
            a.username = :p01
         """;
         
      else:
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
               ,SUM(bb.bytes_used) AS bytes_used
               ,SUM(CASE WHEN bb.compression = 'NONE' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_none
               ,SUM(CASE WHEN bb.compression = 'LOW'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_low
               ,SUM(CASE WHEN bb.compression = 'HIGH' THEN bb.bytes_used ELSE 0 END) AS bytes_comp_high
               ,SUM(CASE WHEN bb.compression = 'UNK'  THEN bb.bytes_used ELSE 0 END) AS bytes_comp_unk
               FROM
               segments_compression bb
               GROUP BY
               bb.owner
            ) b
            ON
            a.username = b.owner
            WHERE
            a.username = :p01
         """;
         
      curs.execute(
          str_sql
         ,{'p01':self._schema_name}    
      );
      for row in curs:
         schema_name     = row[0];
         bytes_used      = row[1];
         bytes_comp_none = row[2];
         bytes_comp_low  = row[3];
         bytes_comp_high = row[4];
         bytes_comp_unk  = row[5];
      
      self._bytes_used      = bytes_used
      self._bytes_comp_none = bytes_comp_none
      self._bytes_comp_low  = bytes_comp_low
      self._bytes_comp_high = bytes_comp_high
      self._bytes_comp_unk  = bytes_comp_unk
            
      curs.close();
      
############################################################################### 
class ResourceGroup(object):

   def __init__(
       self
      ,parent
      ,resource_group_name
   ):
   
      self._parent              = parent;
      self._sqliteconn          = parent._sqliteconn;
      self._resource_group_name = resource_group_name;
      self._resources           = {};
      
   @property
   def dataset_name(self):
      return self._dataset_name;
      
   @property
   def resources(self):
      return self._resources;
      
   @property
   def resources_l(self):
      return [d for d in self.resources.values()];
      
   @property
   def bytes_used(self):
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_used;
      return rez;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_none(self):
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_none;
      return rez;
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_low;
      return rez;
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_high;
      return rez;
      
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_unk(self):
      rez = 0;
      for item in self.resources.values():
         rez += item.bytes_comp_unk;
      return rez;
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
      
   ############################################################################
   def add_resource(
       self
      ,table_owner
      ,table_name
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
      ,table_owner
      ,table_name
   ):
   
      if self._resources is None:
         self._resources = {};

      if (table_owner,table_name) in self._resources:
         del self._resources[(table_owner,table_name)];
      
############################################################################### 
class Resource(object):

   def __init__(
       self
      ,parent
      ,table_owner
      ,table_name
   ):
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._table_owner     = table_owner;
      self._table_name      = table_name;
      self._secondaries     = {};
      
      # Verify item is eligible resource item
      curs = self._sqliteconn.cursor();
   
      str_sql = """
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
         ,b.bytes_used
         ,CASE WHEN b.compression = 'NONE' THEN b.bytes_used ELSE 0 END AS bytes_comp_none
         ,CASE WHEN b.compression = 'LOW'  THEN b.bytes_used ELSE 0 END AS bytes_comp_low
         ,CASE WHEN b.compression = 'HIGH' THEN b.bytes_used ELSE 0 END AS bytes_comp_high
         ,CASE WHEN b.compression = 'UNK'  THEN b.bytes_used ELSE 0 END AS bytes_comp_unk
         ,a.partitioned
         ,a.iot_type
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
             a.owner         = :p01
         AND a.table_name    = :p02
         AND a.secondary     = 'N'
      """;
      
      curs.execute(
          str_sql
         ,{'p01':table_owner,'p02':table_name}
      );
      table_name = None;
      for row in curs:
         owner           = row[0];
         table_name      = row[1];
         partition_name  = row[2];
         segment_type    = row[3];
         tablespace_name = row[4];
         bytes_used      = row[5];
         bytes_comp_none = row[6];
         bytes_comp_low  = row[7];
         bytes_comp_high = row[8];
         bytes_comp_unk  = row[9];
         partitioned     = row[10];
         iot_type        = row[11];
         isgeor          = row[12];

      # Abend hard if the item does have seconday = 'N'
      if table_name is None:
         raise Exception(self.table_owner + '.' + self.table_name + ' is not a resource.');     

      self._secondaries[(table_owner,table_name,partition_name)] = Secondary(
          parent_resource = self
         ,depth           = 0
         ,owner           = table_owner
         ,segment_name    = table_name
         ,partition_name  = partition_name
         ,segment_type    = segment_type
         ,tablespace_name = tablespace_name
         ,bytes_used      = bytes_used
         ,bytes_comp_none = bytes_comp_none
         ,bytes_comp_low  = bytes_comp_low
         ,bytes_comp_high = bytes_comp_high
         ,bytes_comp_unk  = bytes_comp_unk
         ,partitioned     = partitioned
         ,iot_type        = iot_type
         ,isgeor          = isgeor
      );
            
      curs.close();
            
   @property
   def table_owner(self):
      return self._table_owner;

   @property
   def table_name(self):
      return self._table_name;
      
   @property
   def secondaries(self):
      return self._secondaries;
      
   @property
   def secondaries_l(self):
      return [d for d in self.secondaries.values()];
      
   @property
   def bytes_used(self):
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_used;
      return rez;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_none(self):
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_comp_none;
      return rez;
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_comp_low;
      return rez;
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_comp_high;
      return rez;
   
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
   
   @property
   def bytes_comp_unk(self):
      rez = 0;
      for item in self.secondaries.values():
         rez += item.bytes_comp_unk;
      return rez;
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
      
############################################################################### 
class Secondary(object):

   def __init__(
       self
      ,parent_resource
      ,depth
      ,owner
      ,segment_name
      ,partition_name  = None
      ,segment_type    = None
      ,tablespace_name = None
      ,bytes_used      = None
      ,bytes_comp_none = None
      ,bytes_comp_low  = None
      ,bytes_comp_high = None
      ,bytes_comp_unk  = None
      ,index_type      = None
      ,ityp_owner      = None
      ,ityp_name       = None
      ,partitioned     = None
      ,iot_type        = None
      ,isgeor          = None
   ):
   
      self._parent_resource = parent_resource;
      self._owner           = owner;
      self._depth           = depth;
      self._segment_name    = segment_name;
      self._partition_name  = partition_name;
      self._segment_type    = segment_type;
      self._tablespace_name = tablespace_name;
      
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
            bytes_used      = row[5];
            bytes_comp_none = row[6];
            bytes_comp_high = row[7];
            bytes_comp_low  = row[8];
            bytes_comp_unk  = row[9];
         
            if (owner,segment_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(owner,segment_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,owner           = owner
                  ,depth           = depth + 1
                  ,segment_name    = segment_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
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
            bytes_used      = row[5];
            bytes_comp_none = row[6];
            bytes_comp_low  = row[7];
            bytes_comp_high = row[8];
            bytes_comp_unk  = row[9];
            index_type      = row[10];
            ityp_owner      = row[11];
            ityp_name       = row[12];
            
            if (index_owner,index_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(index_owner,index_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,owner           = index_owner
                  ,depth           = depth + 1
                  ,segment_name    = index_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
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
               table_owner     = row[0];
               table_name      = row[1];
               partition_name  = row[2];
               segment_type    = row[3];
               tablespace_name = row[4];
               bytes_used      = row[5];
               bytes_comp_none = row[6];
               bytes_comp_low  = row[7];
               bytes_comp_high = row[8];
               bytes_comp_unk  = row[9];
               iot_type        = row[10];
               isgeor          = row[11];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource = parent_resource
                     ,depth           = depth + 1
                     ,owner           = table_owner
                     ,segment_name    = table_name
                     ,partition_name  = partition_name
                     ,segment_type    = segment_type
                     ,tablespace_name = tablespace_name
                     ,bytes_used      = bytes_used
                     ,bytes_comp_none = bytes_comp_none
                     ,bytes_comp_low  = bytes_comp_low
                     ,bytes_comp_high = bytes_comp_high
                     ,bytes_comp_unk  = bytes_comp_unk
                     ,iot_type        = iot_type
                     ,isgeor          = isgeor
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
               table_owner     = row[0];
               table_name      = row[1];
               partition_name  = row[2];
               segment_type    = row[3];
               tablespace_name = row[4];
               bytes_used      = row[5];
               bytes_comp_none = row[6];
               bytes_comp_low  = row[7];
               bytes_comp_high = row[8];
               bytes_comp_unk  = row[9];
               iot_type        = row[10];
               isgeor          = row[11];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource = parent_resource
                     ,depth           = depth + 1
                     ,owner           = table_owner
                     ,segment_name    = table_name
                     ,partition_name  = partition_name
                     ,segment_type    = segment_type
                     ,tablespace_name = tablespace_name
                     ,bytes_used      = bytes_used
                     ,bytes_comp_none = bytes_comp_none
                     ,bytes_comp_low  = bytes_comp_low
                     ,bytes_comp_high = bytes_comp_high
                     ,bytes_comp_unk  = bytes_comp_unk
                     ,iot_type        = iot_type
                     ,isgeor          = isgeor
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
               table_owner     = row[0];
               table_name      = row[1];
               partition_name  = row[2];
               segment_type    = row[3];
               tablespace_name = row[4];
               bytes_used      = row[5];
               bytes_comp_none = row[6];
               bytes_comp_low  = row[7];
               bytes_comp_high = row[8];
               bytes_comp_unk  = row[9];
               iot_type        = row[10];
               isgeor          = row[11];
               
               if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
                  parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                      parent_resource = parent_resource
                     ,depth           = depth + 1
                     ,owner           = table_owner
                     ,segment_name    = table_name
                     ,partition_name  = partition_name
                     ,segment_type    = segment_type
                     ,tablespace_name = tablespace_name
                     ,bytes_used      = bytes_used
                     ,bytes_comp_none = bytes_comp_none
                     ,bytes_comp_low  = bytes_comp_low
                     ,bytes_comp_high = bytes_comp_high
                     ,bytes_comp_unk  = bytes_comp_unk
                     ,iot_type        = iot_type
                     ,isgeor          = isgeor
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
            table_owner     = row[0];
            table_name      = row[1];
            partition_name  = row[2];
            segment_type    = row[3];
            tablespace_name = row[4];
            bytes_used      = row[5];
            bytes_comp_none = row[6];
            bytes_comp_low  = row[7];
            bytes_comp_high = row[8];
            bytes_comp_unk  = row[9];
            
            if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,depth           = depth + 1
                  ,owner           = table_owner
                  ,segment_name    = table_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
                  ,bytes_used      = bytes_used
                  ,bytes_comp_none = bytes_comp_none
                  ,bytes_comp_low  = bytes_comp_low
                  ,bytes_comp_high = bytes_comp_high
                  ,bytes_comp_unk  = bytes_comp_unk
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
            table_owner     = row[0];
            table_name      = row[1];
            partition_name  = row[2];
            segment_type    = row[3];
            tablespace_name = row[4];
            bytes_used      = row[5];
            bytes_comp_none = row[6];
            bytes_comp_low  = row[7];
            bytes_comp_high = row[8];
            bytes_comp_unk  = row[9];
            isgeor          = row[10];
            
            if (table_owner,table_name,partition_name) not in parent_resource._secondaries:
               parent_resource._secondaries[(table_owner,table_name,partition_name)] = Secondary(
                   parent_resource = parent_resource
                  ,depth           = depth + 1
                  ,owner           = table_owner
                  ,segment_name    = table_name
                  ,partition_name  = partition_name
                  ,segment_type    = segment_type
                  ,tablespace_name = tablespace_name
                  ,bytes_used      = bytes_used
                  ,bytes_comp_none = bytes_comp_none
                  ,bytes_comp_low  = bytes_comp_low
                  ,bytes_comp_high = bytes_comp_high
                  ,bytes_comp_unk  = bytes_comp_unk
                  ,isgeor          = isgeor
               );
      
      curs.close();
      
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
   def bytes_used(self):
      return self._bytes_used;
      
   @property
   def gb_used(self):
      return self.bytes_used / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_none(self):
      return self._bytes_comp_none;
      
   @property
   def gb_comp_none(self):
      return self.bytes_comp_none / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_low(self):
      return self._bytes_comp_low;
      
   @property
   def gb_comp_low(self):
      return self.bytes_comp_low / 1024 / 1024 / 1024;
      
   @property
   def bytes_comp_high(self):
      return self._bytes_comp_high;
   
   @property
   def gb_comp_high(self):
      return self.bytes_comp_high / 1024 / 1024 / 1024;
   
   @property
   def bytes_comp_unk(self):
      return self._bytes_comp_unk;
      
   @property
   def gb_comp_unk(self):
      return self.bytes_comp_unk / 1024 / 1024 / 1024;
