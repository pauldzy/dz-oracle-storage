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
            ,PRIMARY KEY(owner,table_name)
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
         ) VALUES (
            ?,?,?,?,?,?
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
         FROM
         dba_tables a
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
         ) VALUES (
            ?,?,?,?,?,?,?,?,?
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
            
############################################################################### 
def slugify(value, allow_unicode=False):
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
   