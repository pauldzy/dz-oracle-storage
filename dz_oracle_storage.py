import os,sys;
import sqlite3,cx_Oracle;
from datetime import datetime;
import unicodedata,re;

class Instance(object):

   tsf = 'YYYY-MM-DD HH24:MI:SS.FF6 TZR';
   
   def __init__(
       self
      ,name
      ,username
      ,password
      ,hoststring
      ,use_flashback = False
   ):
   
      self._name          = name;
      self._username      = username;
      self._password      = password;
      self._hoststring    = hoststring;
      self._use_flashback = use_flashback;
      
      self._dts           = None;
      self._dts_s         = None;
      
      self._orcl          = None;
      self._has_spatial   = None;
      self._has_text      = None;
      self._has_sde       = None;      
      self.initorcl();
      
      self._sqlitepath  = os.path.dirname(os.path.abspath(__file__)) + os.sep + slugify(name) + '.db';
      self._sqliteconn  = None;
      
      self.deletesqlite();
      self.initsqlite();
      
      self.loadorcl();
      
      self._bytes_allocated = None;
      self._bytes_used      = None;
      self._bytes_free      = None;
      self._tablespaces     = None;
      self._schemas         = None;
      self._datasets        = None;
      
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
   def schemas(self): 
      if self._schemas is None or self._schemas == {}:
         self.loadschemas();
      return self._schemas;
         
   @property
   def schemas_l(self):
      return [d for d in self.schemas.values()];
      
   @property
   def datasets(self): 
      if self._datasets is None:
         self.datasets = {};
      return self._datasets;
         
   @property
   def datasets_l(self):
      return [d for d in self.datasets.values()];
      
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
         sys.stderr.write("    username: " + str(p_username) + "\n");
         sys.stderr.write("    password: XXXXXXXX\n");
         sys.stderr.write("  hoststring: " + str(p_hoststring) + "\n");
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
         
         CREATE TABLE sde_st_geometry_columns(
             owner           TEXT    NOT NULL
            ,table_name      TEXT    NOT NULL
            ,column_name     TEXT    NOT NULL
            ,geom_id         TEXT    NOT NULL
            ,PRIMARY KEY(owner,table_name,column_name)
         );
         
         CREATE VIEW segments_compression(
             owner
            ,segment_name
            ,partition_name
            ,segment_type
            ,tablespace_name
            ,bytes_used
            ,compression
            ,iot_type
            ,secondary
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
            ccc.iot_type
          ELSE
            NULL
          END AS iot_type
         ,CASE
          WHEN bbb.segment_type = 'TABLE'
          THEN
            ccc.secondary
          ELSE
            NULL
          END AS secondary
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
         AND bbb.partition_name = eee.partition_name;
      
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
            ,iot_type
            ,secondary
         ) VALUES (
            ?,?,?,?,?,?,?
         )
      """;
      
      str_from = """
         SELECT
          a.owner
         ,a.table_name
         ,a.num_rows
         ,a.tablespace_name
         ,a.compress_for
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
            INSERT INTO sde_st_geometry_columns(
                owner
               ,table_name
               ,column_name
               ,geom_id
            ) VALUES (
               ?,?,?,?
            )
         """;
         
         str_from = """
            SELECT
             a.owner
            ,a.table_name
            ,a.column_name
            ,a.geom_id
            FROM
            sde.st_geometry_columns a
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
         ,b.bytes_used
         ,b.bytes_comp_none
         ,b.bytes_comp_low
         ,b.bytes_comp_high
         ,b.bytes_comp_unk
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
   def add_dataset(
       self
      ,dataset_name
   ):
   
      if self._datasets is None:
         self._datasets = {};
         
      self._datasets[dataset_name] = Dataset(
          parent       = self
         ,dataset_name = dataset_name
      );
      
   ############################################################################
   def delete_dataset(
       self
      ,dataset_name
   ):
   
      if self._datasets is None:
         self._datasets = {};

      if dataset_name in self._datasets:
         del self._datasets[dataset_name];
         
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
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._schema_name     = schema_name;
      self._bytes_used      = bytes_used;
      self._bytes_comp_none = bytes_comp_none;
      self._bytes_comp_low  = bytes_comp_low;
      self._bytes_comp_high = bytes_comp_high;
      self._bytes_comp_unk  = bytes_comp_unk;
      
   @property
   def schema_name(self):
      return self._schema_name;
      
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
      
############################################################################### 
class Dataset(object):

   def __init__(
       self
      ,parent
      ,dataset_name
   ):
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._dataset_name    = dataset_name;
      self._resources       = {};
      
   @property
   def dataset_name(self):
      return self._dataset_name;
      
   @property
   def resources(self):
      return self._resources;
      
   @property
   def resources_l(self):
      return [d for d in self.resources.values()];
      
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
      self._secondaries     = [];
      
      # Verify item is eligible resource item
      curs = self._sqliteconn.cursor();
   
      str_sql = """
         SELECT
          a.partition_name
         ,a.segment_type
         ,a.tablespace_name
         ,a.bytes_used
         ,CASE WHEN a.compression = 'NONE' THEN a.bytes_used ELSE 0 END AS bytes_comp_none
         ,CASE WHEN a.compression = 'LOW'  THEN a.bytes_used ELSE 0 END AS bytes_comp_low
         ,CASE WHEN a.compression = 'HIGH' THEN a.bytes_used ELSE 0 END AS bytes_comp_high
         ,CASE WHEN a.compression = 'UNK'  THEN a.bytes_used ELSE 0 END AS bytes_comp_unk
         ,a.iot_type
         FROM 
         segments_compression a
         WHERE
             a.owner        = :p01
         AND a.segment_name = :p02
         AND a.secondary    = 'N'
      """;
      
      curs.execute(
          str_sql
         ,{'p01':table_owner,'p02':table_name}
      );
      segment_type = None;
      for row in curs:
         partition_name  = row[0];
         segment_type    = row[1];
         tablespace_name = row[2];
         bytes_used      = row[3];
         bytes_comp_none = row[4];
         bytes_comp_low  = row[5];
         bytes_comp_high = row[6];
         bytes_comp_unk  = row[7];
         iot_type        = row[8];

      # Abend hard if the item does have seconday = 'N'
      if segment_type is None:
         raise Exception(self.table_owner + '.' + self.table_name + ' is not a resource.');     
      
      self._secondaries.append(
         Secondary(
             parent          = self
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
         )
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
   def secondary_count(self):
      return len(self._secondaries);
      
############################################################################### 
class Secondary(object):

   def __init__(
       self
      ,parent
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
   ):
   
      self._parent          = parent;
      self._sqliteconn      = parent._sqliteconn;
      self._owner           = owner;
      self._depth           = depth;
      self._segment_name    = segment_name;
      self._partition_name  = partition_name;
      self._segment_type    = segment_type;
      self._tablespace_name = tablespace_name
      self._bytes_used      = bytes_used;
      self._bytes_comp_none = bytes_comp_none;
      self._bytes_comp_low  = bytes_comp_low;
      self._bytes_comp_high = bytes_comp_high;
      self._bytes_comp_unk  = bytes_comp_unk;
      
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
           
############################################################################### 
def slugify(value,allow_unicode=False):
   """
   Taken from https://github.com/django/django/blob/master/django/utils/text.py
   Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
   dashes to single dashes. Remove characters that aren't alphanumerics,
   underscores, or hyphens. Convert to lowercase. Also strip leading and
   trailing whitespace, dashes, and underscores.
   """
   value = str(value);
   if allow_unicode:
      value = unicodedata.normalize('NFKC',value);
   else:
      value = unicodedata.normalize('NFKD',value).encode('ascii','ignore').decode('ascii');
   value = re.sub(r'[^\w\s-]', '', value.lower());
   return re.sub(r'[-\s]+', '-', value).strip('-_');
   