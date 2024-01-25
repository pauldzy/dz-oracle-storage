import sys;
import os;
import random;
import re;
import oracledb;

def get_orcl_handle(
    p_username
   ,p_password
   ,p_hoststring
):

   try:
      orcl = oracledb.connect( 
          user     = p_username
         ,password = p_password
         ,dsn      = p_hoststring 
         ,encoding = "UTF-8"   
      );
      
   except oracledb.DatabaseError as e:
      sys.stderr.write("ERROR, unable to log into Oracle with \n");
      sys.stderr.write("    username: " + str(p_username) + "\n");
      sys.stderr.write("    password: XXXXXXXX\n");
      sys.stderr.write("  hoststring: " + str(p_hoststring) + "\n");
      sys.stderr.write("  oracle msg: " + str(e) + "\n");
      sys.exit(-1);

   curs = orcl.cursor();
   str_sql  = "SELECT ";
   str_sql += "SYSTIMESTAMP ";
   str_sql += "FROM ";
   str_sql += "dual ";
   try:
      curs.execute(str_sql);
      
   except oracledb.DatabaseError as e:
      sys.stderr.write(str_sql + "\n");
      raise;

   curs.close();

   return orcl;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def is_user_table(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):

   if p_orcl is None:
      raise Exception("ERROR, oracle handle is empty!\n");

   if p_table_name is None:
      raise Exception("ERROR, table name parameter is empty!\n");

   str_sql  = "SELECT ";
   str_sql += "COUNT(*) ";
   str_sql += "FROM ";
   str_sql += "user_tables a ";
   str_sql += "WHERE ";
   str_sql += "a.table_name = :p01 ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(p_table_name)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();
   
   curs.close();
   
   if row[0] == 1:
      return True;
   else:
      return False;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def check_table_accessible(
    p_orcl
   ,p_input
   ,p_check_if_table = False
   ,p_tracing = None
):

   schema,table_name = split2schematable(p_orcl,p_input);

   if schema is None and table_name is None:
      raise Exception("ERROR! cannot parse schema and table_name from input!\n");

   elif schema is None and table_name is not None:
      str_sql  = "SELECT ";
      str_sql += "COUNT(*) ";
      str_sql += "FROM ";
      str_sql += "user_objects ";
      str_sql += "WHERE ";
      str_sql += "a.object_name = :p1 AND ";
      
      if p_check_if_table is True:
         str_sql += "a.object_type = 'TABLE' ";
      else:
         str_sql += "a.object_type IN ('TABLE','VIEW') ";
      
      curs = ez_execute(
          p_orcl
         ,str_sql
         ,{'p1':str(table_name)}
         ,p_tracing = p_tracing
      );

   else:
      str_sql  = "SELECT ";
      str_sql += "COUNT(*) ";
      str_sql += "FROM ";
      str_sql += "all_objects a ";
      str_sql += "WHERE ";
      str_sql += "    a.owner = :p01 ";
      str_sql += "AND a.object_name = :p02 ";
      
      if p_check_if_table is True:
         str_sql += "AND a.object_type = 'TABLE' ";
      else:
         str_sql += "AND a.object_type IN ('TABLE','VIEW') ";
 
      curs = ez_execute(
          p_orcl
         ,str_sql
         ,{'p01':str(schema),'p02':str(table_name)}
         ,p_tracing = p_tracing
      );

   row = curs.fetchone();

   curs.close();

   if row[0] == 0:
      return False;
   else:
      return True;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def check_table_exists(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):

   return check_table_accessible(
       p_orcl    = p_orcl
      ,p_input   = p_table_name
      ,p_tracing = p_tracing
   );

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def check_tablespace(
    p_orcl
   ,p_tablespace
   ,p_dba = "USER"
   ,p_tracing = None
):

   if p_orcl is None:
      raise Exception("ERROR, oracle handle is empty.\n");

   if p_tablespace is None:
      raise Exception("ERROR, tablespace name parameter is empty.\n");
      
   if p_dba != "USER":
      if p_dba != "DBA":
         raise Exception("ERROR, tablespace dba name can only be USER or DBA.\n");

   str_sql  = "SELECT ";
   str_sql += "COUNT(*) AS tcount ";
   str_sql += "FROM ";
   str_sql += p_dba + "_tablespaces a ";
   str_sql += "WHERE ";
   str_sql += "a.tablespace_name = :p01 ";

   curs = ez_execute(
      p_orcl
     ,str_sql
     ,{'p01':str(p_tablespace)}
     ,p_tracing = p_tracing
   );

   row = curs.fetchone();
   
   curs.close();

   if row[0] == 1:
      return True;
   else:
      return False;
      
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_tablespace_name(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):

   schema,table_name = split2schematable(p_orcl,p_table_name);

   str_sql  = "SELECT "
   str_sql += "a.tablespace_name "
   str_sql += "FROM "
   str_sql += "all_tables a "
   str_sql += "WHERE "
   str_sql += "    a.owner = :p01 ";
   str_sql += "AND a.table_name = :p02 ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(table_name)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();
   curs.close();

   if row is None:
      sys.stderr.write("ERROR, unable to detect a tablespace for table " + str(p_table_name) + ".\n");
   else:
      return str(row[0]);

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def check_datafile_exists(
    p_orcl
   ,p_datafile
   ,p_tracing = None
):

   str_sql  = "SELECT "
   str_sql += "b.name "
   str_sql += "FROM "
   str_sql += "v$datafile a "
   str_sql += "JOIN "
   str_sql += "v$tablespace b "
   str_sql += "ON "
   str_sql += "a.ts# = b.ts# "
   str_sql += "WHERE "
   str_sql += "a.name = :p01 "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(p_datafile)}
      ,{'ORA-00942':'ERROR, user ' + p_orcl.username + ' does not have SELECT privileges on the v$datafile or v$tablespace tables.\nGrant the privilege or use a different schema user.'}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();
   curs.close();

   if row is None:
      return None;
   else:
      return str(row[0]);

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_grants(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):

   ary_output = [];

   str_sql  = "SELECT "
   str_sql += " a.grantee "
   str_sql += ",a.owner "
   str_sql += ",a.table_name "
   str_sql += ",a.grantor "
   str_sql += ",a.privilege "
   str_sql += ",a.grantable "
   str_sql += ",a.hierarchy "
   str_sql += "FROM "
   str_sql += "user_tab_privs a "
   str_sql += "WHERE "
   str_sql += "a.table_name = :p01 "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(p_table_name)}
      ,p_tracing = p_tracing
   );

   for grantee,owner,table_name,grantor,privilege,grantable,hierarchy in curs.fetchall():
      ary_output.append(
         (grantee,owner,table_name,grantor,privilege,grantable,hierarchy)
      );

   return ary_output;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_object_ddl(
    p_orcl
   ,p_object_type
   ,p_object_name
   ,p_tracing = None
):

   str_sql  = "SELECT "
   str_sql += "DBMS_METADATA.GET_DDL(:p01,:p02) "
   str_sql += "FROM "
   str_sql += "dual ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(p_object_type),'p02':str(p_object_name)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   curs.close();

   return str(row[0]);

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def split2schematable(
    p_orcl
   ,p_input
):

   if p_input is None:
      raise Exception("ERROR, p_input is empty!");

   tmp_array = p_input.split('.');
   if len(tmp_array) == 1:
      return (p_orcl.username,p_input);
   elif len(tmp_array) == 2:
      return (str(tmp_array[0]),str(tmp_array[1]));
   else:
      return (None,None);

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_table_list (
    p_orcl
   ,p_table_name
   ,p_schema       = None
   ,p_tracing      = None
):
   output = [];
   str_sql  = "SELECT "
   str_sql += "a.table_name "
   str_sql += "FROM ";
   str_sql += "all_tables a "
   str_sql += "WHERE "
   str_sql += "    a.owner = :p01 ";
   str_sql += "AND REGEXP_LIKE(a.table_name,:p02) ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':p_orcl.username,'p02':str(p_table_name)}
      ,p_tracing = p_tracing
   );

   for table_name, in curs.fetchall():
      output.append(table_name);

   return output;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def dzy(p_hash,p_in):

   if p_hash is None or p_hash == {}:
      return p_in;
      
   for key,val in p_in.items():
   
      if key in p_hash:
         p_hash[key] += val;
         
      else:
         p_hash[key] = val;
         
   return p_hash;
      
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_table_size2(
    p_orcl
   ,p_table_name
   ,p_tracing = None
   ,p_audit = False
   ,p_check_sde = False
):

   table_size = {};

   # First get table size alone
   table_size = dzy(table_size,get_object_size2(p_orcl,p_table_name,p_tracing,p_audit));

   # Second, get the table LOBs
   table_size = dzy(table_size,get_table_lob_size2(p_orcl,p_table_name,p_tracing,p_audit));

   # Third, get size of all nondomain indexes
   indexes = get_table_indexes(p_orcl,p_table_name,'N',p_tracing);
   for this_index in indexes:
      table_size = dzy(table_size,get_object_size2(p_orcl,this_index,p_tracing,p_audit));

   # Fourth, get size of all domain indexes that are known to us
   indexes = get_table_indexes(p_orcl,p_table_name,'Y',p_tracing);
   for this_index in indexes:
      table_size = dzy(table_size,get_domain_index_size2(p_orcl,this_index,p_tracing,p_audit));

   # Fifth, check if table is SDE table and get sizes of all objects
   if p_check_sde is True:
      table_size = dzy(table_size,get_sde_table_size2(p_orcl,p_table_name,p_tracing,p_audit));

   # Check if table is complex object table, SDO_GEORASTER is the only type for now
   if is_complex_object_table(p_orcl,p_table_name,p_tracing) == "SDO_GEORASTER":
      child_tables = [];
      child_tables = get_sdo_georaster_tables(p_orcl,p_table_name,p_tracing);
      
      for child in child_tables:
         table_size = dzy(table_size,get_table_size2(p_orcl,child,p_tracing,p_audit));

   return table_size;
   
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def dzcomp(
   p_in
):

   if p_in is None or p_in == "" or p_in in ("NONE","NULL","DISABLED","NO"):
      return "NONE";
      
   elif p_in in ("LOW","BASIC","QUERY LOW","ARCHIVE LOW","ADVANCED LOW","ENABLED"):
      return "LOW";
      
   elif p_in in ("MEDIUM"):
      return "MEDIUM";
      
   elif p_in in ("HIGH","ADVANCED","QUERY HIGH","ARCHIVE HIGH","ADVANCED HIGH"):
      return "HIGH"
      
   else:
      raise Exception("Unknown compression value " + str(p_in));

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   
def get_object_size2(
    p_orcl
   ,p_segment_name
   ,p_tracing = None
   ,p_audit = False
   ,p_override_compression = None
):

   schema,table_name = split2schematable(p_orcl,p_segment_name);
   
   #-- First check if the object is an indexed organized table
   curs   = p_orcl.cursor();
   str_sql  = "SELECT "
   str_sql += " a.iot_type "
   str_sql += ",b.owner "
   str_sql += ",b.index_name "
   str_sql += ",a.compress_for AS table_compression "
   str_sql += ",b.compression  AS index_compression "
   str_sql += "FROM "
   str_sql += "all_tables a "
   str_sql += "LEFT JOIN "
   str_sql += "( "
   str_sql += "   SELECT "
   str_sql += "    bb.owner "
   str_sql += "   ,bb.index_name "
   str_sql += "   ,bb.table_name "
   str_sql += "   ,bb.table_owner "
   str_sql += "   ,bb.compression "
   str_sql += "   FROM "
   str_sql += "   all_indexes bb "
   str_sql += "   WHERE "
   str_sql += "   bb.index_type = 'IOT - TOP' "
   str_sql += ") b "
   str_sql += "ON "
   str_sql += "a.owner = b.table_owner AND "
   str_sql += "a.table_name = b.table_name "   
   str_sql += "WHERE "
   str_sql += "a.owner = :p01 AND "
   str_sql += "a.table_name = :p02 ";
   
   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(table_name)}
      ,p_tracing = p_tracing
   );
                    
   row = curs.fetchone();
   
   iot_flag = False;
   if row is not None:
      if row[0] == "IOT":
         # -- If its an IOT then we need to flag the bytes to zero as the index covers the size
         iot_flag    = True;
         schema      = row[1];
         table_name  = row[2];
         compression = dzcomp(row[4]);
 
   curs   = p_orcl.cursor();
   str_sql  = "SELECT "
   str_sql += " a.bytes "
   str_sql += ",a.tablespace_name "
   str_sql += ",a.segment_type "
   str_sql += ",b.num_rows "
   str_sql += ",b.compress_for "
   str_sql += "FROM "
   str_sql += "dba_segments a "
   str_sql += "LEFT JOIN "
   str_sql += "all_tables b "
   str_sql += "ON "
   str_sql += "    a.segment_name = b.table_name "
   str_sql += "AND a.owner = b.owner "
   str_sql += "WHERE "
   str_sql += "    a.owner = :p01 "
   str_sql += "AND a.segment_name = :p02 ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(table_name)}
      ,{'ORA-00942':'ERROR, this user does not have SELECT privileges on the DBA_SEGMENTS table.\nGrant the privilege or use a different schema user.'}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      #--- For 11g, we have delayed segment creation so nothing in dba_segments means size 0
      bytes       = 0;
      compression = "NONE";
   
   else:
      if iot_flag is True:
         bytes = 0;
      else:
         bytes       = row[0];
         
      tablespace  = row[1];
      seg_type    = row[2];
      num_rows    = row[3];
      
      if p_override_compression is not None:
         compression = p_override_compression;
      else:
         compression = dzcomp(row[4]);
   
   if p_audit is True:

      print("   <object type=\"" + str(seg_type) + "\">");
      print("      <name>" + str(p_segment_name) + "</name>");
      print("      <size unit=\"byte\">" + str(bytes) + "</size>");
      if tablespace is not None:
         print("      <tablespace>" + str(tablespace) + "</tablespace>");
      if num_rows is not None:
         print("      <rows>" + str(num_rows) + "</rows>");
      if compression is not None:
         print("      <compression>" + str(compression) + "</compression>");
      print("   </object>");
   
   if compression is None:
      compression = "NONE";
   
   return {
      compression: bytes 
   };

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_table_indexes(
    p_orcl
   ,p_table_name
   ,p_domain_flag = 'ALL'
   ,p_tracing = None
):

   schema,table_name = split2schematable(p_orcl,p_table_name);

   curs   = p_orcl.cursor();

   str_sql  = "SELECT ";
   str_sql += " a.owner ";
   str_sql += ",a.index_name ";
   str_sql += "FROM ";
   str_sql += "all_indexes a ";
   str_sql += "WHERE ";
   str_sql += "    a.table_owner = :p01 ";
   str_sql += "AND a.table_name = :p02 ";

   if p_domain_flag == 'N':
      str_sql += "AND a.index_type != 'DOMAIN' ";
   elif p_domain_flag == 'Y':
      str_sql += "AND a.index_type = 'DOMAIN' ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(table_name)}
      ,p_tracing = p_tracing
   );

   output = [];
   for z1,z2 in curs.fetchall():
      output.append(str(z1) + "." + str(z2));

   curs.close()

   return output;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_domain_index_size2(
    p_orcl
   ,p_domain_index_name
   ,p_tracing = None
   ,p_audit = False
):
   num_running_total = {};
   schema,index_name = split2schematable(p_orcl,p_domain_index_name);

   str_sql  = "SELECT ";
   str_sql += " a.owner ";
   str_sql += ",a.index_name ";
   str_sql += ",a.ityp_owner ";
   str_sql += ",a.ityp_name ";
   str_sql += ",a.table_owner ";
   str_sql += ",a.table_name ";
   str_sql += ",a.compression ";
   str_sql += "FROM ";
   str_sql += "all_indexes a ";
   str_sql += "WHERE ";
   str_sql += "    a.owner = :p01 ";
   str_sql += "AND a.index_name = :p02 ";
   str_sql += "AND a.index_type = 'DOMAIN' ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(index_name)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      raise Exception("ERROR, could not find a domain index named " + str(p_domain_index_name) + "!");

   i_owner,i_name,ityp_owner,ityp_name,t_owner,t_name,compression = row;

   if ityp_owner == 'MDSYS' and ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
      # Its an Oracle spatial index
      str_sql  = "SELECT "
      str_sql += " a.sdo_index_owner "
      str_sql += ",a.sdo_index_table "
      str_sql += "FROM "
      str_sql += "mdsys.sdo_index_metadata_table a "
      str_sql += "WHERE "
      str_sql += "    a.sdo_index_owner = :p01 "
      str_sql += "AND a.sdo_index_name = :p02 ";

      curs = ez_execute( 
          p_orcl
         ,str_sql
         ,{'p01':str(i_owner),'p02':str(i_name)}
         ,{'ORA-00942':'ERROR, this user does not have SELECT privileges on the mdsys.sdo_index_metadata_table table.\nGrant the privilege or use a different schema user.'}
         ,p_tracing=p_tracing
      );
      chk_return = curs.fetchone();
      
      if chk_return is None:
         print("SCHEMA VALIDITY ERROR, spatial index " + str(i_name) + " does not seem to be valid.");
         
      else:
         spatial_index_owner,spatial_index_table = chk_return;
         
         num_running_total = dzy(num_running_total,get_table_size2(
             p_orcl
            ,spatial_index_owner + "." + spatial_index_table
            ,p_tracing
            ,p_audit
         ));
         
         str_mdxt_table = spatial_index_table.replace("MDRT","MDXT");
         if check_table_accessible(p_orcl,spatial_index_owner + "." + str_mdxt_table) is True:
            num_running_total = dzy(num_running_total,get_table_size2(
                p_orcl
               ,spatial_index_owner + "." + str_mdxt_table
               ,p_tracing
               ,p_audit
            ));
            
         str_mdxt_table = spatial_index_table.replace("MDRT","MDTP");
         if check_table_accessible(p_orcl,spatial_index_owner + "." + str_mdxt_table) is True:
            num_running_total = dzy(num_running_total,get_table_size2(
                p_orcl
               ,spatial_index_owner + "." + str_mdxt_table
               ,p_tracing
               ,p_audit
            ));
         
      return num_running_total;

   elif ityp_owner == 'SDE' and ityp_name == 'ST_SPATIAL_INDEX':

      # Its an ESRI ST_GEOMETRY spatial index
      str_sql  = "SELECT "
      str_sql += "a.geom_id "
      str_sql += "FROM "
      str_sql += "sde.st_geometry_columns a "
      str_sql += "WHERE "
      str_sql += "    a.owner = :p01 "
      str_sql += "AND a.table_name = :p02 ";

      curs = ez_execute(
          p_orcl
         ,str_sql
         ,{'p01':str(t_owner),'p02':str(t_name)}
         ,{'ORA-00942':'ERROR, this user does not have SELECT privileges on the sde.st_geometry_columns table.\nGrant the privilege or use a different schema user.'}
         ,p_tracing = p_tracing
      );

      geom_id = curs.fetchone();

      if geom_id is None or len(geom_id) == 0:
         raise Exception("ERROR, could not find an ST_GEOMETRY index for " + str(t_owner) + "." + str(t_name) + "!");

      st_size = {};
      st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".S" + str(geom_id[0]) + "$_IX1",p_tracing,p_audit));
      st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".S" + str(geom_id[0]) + "$_IX2",p_tracing,p_audit));

      return st_size;

   elif ityp_owner == 'CTXSYS' and ityp_name == 'CONTEXT':

      # Its an Oracle full text index
      str_sql  = "SELECT "
      str_sql += "a.idx_name "
      str_sql += "FROM "
      str_sql += "ctxsys.ctx_indexes a "
      str_sql += "WHERE "
      str_sql += "a.idx_table_owner = :p01 AND "
      str_sql += "a.idx_table = :p02 ";

      curs = ez_execute(
          p_orcl
         ,str_sql
         ,{'p01':str(t_owner),'p02':str(t_name)}
         ,{'ORA-00942':'ERROR, this user does not have SELECT privileges on the ctxsys.ctx_user_indexes table.\nGrant the privilege or use a different schema user.'}
         ,p_tracing = p_tracing
      );

      ctx_id = curs.fetchone();

      if ctx_id is None or len(ctx_id) == 0:
         raise Exception("ERROR, could not find an ctxsys context index for " + str(t_owner) + "." + str(t_name) + "!");

      boo_bad  = False
      st_size  = {}
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$I") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$I",p_tracing,p_audit));
         
      else:
         boo_bad = True;

      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$K") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$K",p_tracing,p_audit));
         
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$KD") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$KD",p_tracing,p_audit));
         
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$KR") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$KR",p_tracing,p_audit));
         
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$N") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$N",p_tracing,p_audit));
         
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$U") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$U",p_tracing,p_audit));
 
      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$R") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$R",p_tracing,p_audit));

      if check_table_accessible(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$X") is True:
         st_size = dzy(st_size,get_table_size2(p_orcl,t_owner + ".DR$" + str(ctx_id[0]) + "$X",p_tracing,p_audit));
         
      if boo_bad is True:
         sys.stderr.write("<warning>Full text domain index on " + str(t_owner) + "." + str(t_name) + " is bad!</warning>\n");

      return st_size;

   else:
      raise Exception("ERROR no idea how to handle domain index type " + str(ityp_owner) + "." + str(ityp_name) + "\n");

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_table_lobs(
    p_orcl
   ,p_table_name
   ,p_tracing = None
   ,p_audit = False
):

   schema,table_name = split2schematable(p_orcl,p_table_name);

   str_sql  = "SELECT ";
   str_sql += " a.segment_name ";
   str_sql += ",a.index_name ";
   str_sql += "FROM ";
   str_sql += "all_lobs a ";
   str_sql += "WHERE ";
   str_sql += "    a.owner = :p01 ";
   str_sql += "AND a.table_name = :p02 ";

   curs = ez_execute(p_orcl,str_sql,{'p01':str(schema),'p02':str(table_name)},p_tracing=p_tracing);

   output = [];
   for lob_segment,lob_index in curs.fetchall():
      if lob_segment is not None:
         output.append(schema + "." + lob_segment)
      if lob_index is not None:
         output.append(schema + "." + lob_index)

   return output

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_table_lob_size2(
    p_orcl
   ,p_table_name
   ,p_tracing = None
   ,p_audit = False
):

   lob_size = {};
   schema,table_name = split2schematable(p_orcl,p_table_name);

   str_sql  = "SELECT ";
   str_sql += " a.segment_name ";
   str_sql += ",a.index_name ";
   str_sql += ",a.compression ";
   str_sql += "FROM ";
   str_sql += "all_lobs a ";
   str_sql += "WHERE ";
   str_sql += "    a.owner      = :p01 ";
   str_sql += "AND a.table_name = :p02 ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(schema),'p02':str(table_name)}
      ,p_tracing = p_tracing
   );

   output = [];
   for lob_segment,lob_index,lob_compression in curs.fetchall():
      lob_size = dzy(
          lob_size
         ,get_object_size2(
             p_orcl
            ,schema + "." + lob_segment
            ,p_tracing
            ,p_audit
            ,p_override_compression = dzcomp(lob_compression)
          )
      );
      lob_size = dzy(
          lob_size
         ,get_object_size2(
             p_orcl
            ,schema + "." + lob_index
            ,p_tracing
            ,p_audit
            ,p_override_compression = dzcomp(lob_compression)
          )
      );

   return lob_size;
   
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_sde_table_size2(
    p_orcl
   ,p_table_name
   ,p_tracing = None
   ,p_audit = False
):

   sde_size = {};
   schema,table_name = split2schematable(p_orcl,p_table_name);

   if check_table_accessible(p_orcl,'sde.layers',p_tracing) is False:
      return {};

   str_sql  = "SELECT "
   str_sql += "a.layer_id "
   str_sql += "FROM "
   str_sql += "sde.layers a "
   str_sql += "JOIN "
   str_sql += "sde.dbtune b "
   str_sql += "ON "
   str_sql += "a.layer_config = b.keyword "
   str_sql += "WHERE "
   str_sql += "    b.parameter_name = 'GEOMETRY_STORAGE' "
   str_sql += "AND TO_CHAR(b.config_string) = 'SDELOB' "
   str_sql += "AND a.owner = :p01 ";
   str_sql += "AND a.table_name = :p02 ";
   str_sql += "ORDER BY ";
   str_sql += "a.layer_id ";

   curs = ez_execute(
       p_orcl
      ,str_sql,{'p01':str(schema),'p02':str(table_name)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      return {};

   geom_id = row[0];

   # F table must exist
   z = get_object_size2(p_orcl,schema + ".F" + str(geom_id),p_tracing,p_audit);
   sde_size = dzy(sde_size,z);

   if p_audit is True:
      print("   <object type=\"table\">");
      print("      <name>" + str(schema) + ".F" + str(geom_id) + "</name>");
      print("      <size unit=\"byte\">");
      for key,val in z.items():
         print("         <" + key + ">" + val + "</" + key + ">");
      print("      </size>");
      print("   </object>");

   # S table may or may not exist
   if check_table_accessible(p_orcl,schema + ".S" + str(geom_id),p_tracing) is True:
      z = get_object_size2(p_orcl,schema + ".S" + str(geom_id),p_tracing,p_audit);
      sde_size = dzy(sde_size,z);

      if p_audit is True:
         print("   <object type=\"table\">");
         print("      <name>" + str(schema) + ".S" + str(geom_id) + "</name>");
         print("      <size unit=\"byte\">");
         for key,val in z.items():
            print("         <" + key + ">" + val + "</" + key + ">");
         print("      </size>");
         print("   </object>");

   return sde_size;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_user_tables(
    p_orcl
   ,p_username                 = None
   ,p_exclude_temp_tables      = False
   ,p_exclude_secondary_tables = False
   ,p_exclude_iot_tables       = False
   ,p_tracing                  = None
   ,p_ignore_tablespace        = None
):
   output = [];

   if p_username is None:
      str_username = p_orcl.username;
   else:
      str_username = p_username

   str_sql  = "SELECT "
   str_sql += "a.object_name "
   str_sql += "FROM "
   str_sql += "all_objects a "
   str_sql += "LEFT JOIN "
   str_sql += "all_tables b "
   str_sql += "ON "
   str_sql += "    a.owner = b.owner "
   str_sql += "AND a.object_name = b.table_name "
   str_sql += "LEFT JOIN "
   str_sql += "all_sdo_geor_sysdata c "
   str_sql += "ON "
   str_sql += "    a.owner = c.owner "
   str_sql += "AND a.object_name = c.rdt_table_name "
   str_sql += "WHERE "
   str_sql += "    a.object_type = 'TABLE' "
   str_sql += "AND a.owner = :p01 "

   if p_exclude_temp_tables is True:
      str_sql += "AND a.temporary = 'N' "

   if p_exclude_secondary_tables is True:
      str_sql += "AND a.secondary = 'N' "
      str_sql += "AND c.rdt_table_name IS NULL "

   if p_exclude_iot_tables is True:
      str_sql += "AND b.iot_type IS NULL "
      
   if p_ignore_tablespace is not None:
      str_sql += "AND (b.tablespace_name IS NULL OR b.tablespace_name != '" + p_ignore_tablespace + "') ";
      
   str_sql += "ORDER BY "
   str_sql += "a.object_name "

   curs = ez_execute(p_orcl,str_sql,{'p01':str(str_username)},p_tracing=p_tracing);

   for table_name, in curs.fetchall():
      output.append(table_name);

   curs.close()

   return output;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   
def get_tablespace_tables(
    p_orcl
   ,p_tablespace
   ,p_exclude_temp_tables      = False
   ,p_exclude_secondary_tables = False
   ,p_exclude_iot_tables       = False
   ,p_tracing                  = None
   ,p_ignore_tablespace        = None
):
   output = [];
   
   str_tablespace = p_tablespace;

   str_sql  = "SELECT "
   str_sql += " a.owner "
   str_sql += ",a.object_name "
   str_sql += "FROM "
   str_sql += "all_objects a "
   str_sql += "LEFT JOIN "
   str_sql += "all_tables b "
   str_sql += "ON "
   str_sql += "a.owner = b.owner AND "
   str_sql += "a.object_name = b.table_name "
   str_sql += "LEFT JOIN "
   str_sql += "all_sdo_geor_sysdata c "
   str_sql += "ON "
   str_sql += "a.owner = c.owner AND "
   str_sql += "a.object_name = c.rdt_table_name "
   str_sql += "WHERE "
   str_sql += "    b.tablespace_name = :p01 "

   if p_exclude_temp_tables is True:
      str_sql += "AND a.temporary = 'N' "

   if p_exclude_secondary_tables is True:
      str_sql += "AND a.secondary = 'N' "
      str_sql += "AND c.rdt_table_name IS NULL "

   if p_exclude_iot_tables is True:
      str_sql += "AND b.iot_type IS NULL "
      
   if p_ignore_tablespace is not None:
      str_sql += "AND (b.tablespace_name IS NULL OR b.tablespace_name != '" + p_ignore_tablespace + "') ";
      
   str_sql += "ORDER BY "
   str_sql += "a.object_name "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(str_tablespace)}
      ,p_tracing = p_tracing
   );

   for owner,table_name in curs.fetchall():
      output.append((owner,table_name));

   curs.close()

   return output;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def sql_debug(
    p_str_sql
   ,p_binds = None 
):

   # this should be bulked up to format the sql nicely in the future
   str_return = p_str_sql

   if p_binds is not None:
      keys = p_binds.keys();
      keys.sort();
      
      for item in keys:
         str_return += ",:" + str(item) + " = " + str(p_binds[item]);

   return str_return;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def ez_execute(
    p_orcl
   ,p_str_sql
   ,p_binds       = None
   ,p_errors      = None
   ,p_ignore_list = None
   ,p_tracing     = None
):

   curs   = p_orcl.cursor();

   try:
      if p_binds is not None:
         curs.execute(p_str_sql,p_binds);
      else:
         curs.execute(p_str_sql);
         
   except oracledb.DatabaseError as e:
      if p_tracing is not None:
         p_tracing.write(0,'SQL',sql_debug(p_str_sql,p_binds),e);

      boo_check = False;
      if p_ignore_list is not None and len(p_ignore_list) > 0:
         for check_code in p_ignore_list:
            if check_code in (str(e)):
               boo_check = True;

      if boo_check is False:
         if p_errors is not None:
            keys = p_errors.keys()
            keys.sort()
            for aerror in keys:
               if aerror in (str(e)):
                  raise Exception(str(p_errors[aerror]) + "\n");

         sys.stderr.write(p_str_sql + "\n");
         if p_binds is not None:
            keys = p_binds.keys()
            for binvar in keys:
               sys.stderr.write(binvar + "=>" + str(p_binds[binvar])+ "\n");
         raise;

   if p_tracing is not None:
      p_tracing.write(0,'SQL',sql_debug(p_str_sql,p_binds),None);

   return curs;

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_directory_path(
    p_orcl
   ,p_directory
   ,p_tracing = None
):

   str_sql  = "SELECT ";
   str_sql += "a.directory_path ";
   str_sql += "FROM ";
   str_sql += "all_directories a ";
   str_sql += "WHERE ";
   str_sql += "a.directory_name = :p01 ";

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':str(p_directory)}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();
   curs.close();
   
   if row is None:
      return None;
   elif row[0] is None:
      return None;
   else:
      return row[0];

   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_tablespace_size(
    p_orcl
   ,p_tablespace = None
   ,p_format     = 'ALLOCATED'
   ,p_tracing    = None
):

   def dba_free_format(
       p_row
      ,p_format
   ):
      if p_format == "ALLOCATED":
         if p_row is None:
            return None;
         else:
            return row[1];
            
      elif p_format == "USED":
         if p_row is None:
            return None;
         else:
            return row[2];
            
      elif p_format == "FREE":
         if p_row is None:
            return None;
         else:
            return row[3];
            
      elif p_format == "ALL":
         if p_row is None:
            return (None,None,None);
         else:
            return (row[1],row[2],row[3]);

   if p_tablespace is not None:

      str_sql  = "SELECT "
      str_sql += " NVL(b.tablespace_name,NVL(a.tablespace_name,'UNKNOWN')) "
      str_sql += ",kbytes_alloc/1024 "
      str_sql += ",kbytes_alloc/1024 - NVL(kbytes_free,0)/1024 "
      str_sql += ",NVL(kbytes_free,0)/1024 "
      str_sql += ",(((kbytes_alloc-nvl(kbytes_free,0))/1024)/(kbytes_alloc/1024)) "
      str_sql += "data_files "
      str_sql += "FROM ( "
      str_sql += "   SELECT "
      str_sql += "    SUM(bytes)/1024/1024 AS Kbytes_free "
      str_sql += "   ,MAX(bytes)/1024/1024 AS largest "
      str_sql += "   ,tablespace_name "
      str_sql += "   FROM "
      str_sql += "   sys.dba_free_space "
      str_sql += "   GROUP BY "
      str_sql += "   tablespace_name "
      str_sql += ") a "
      str_sql += "RIGHT JOIN ( "
      str_sql += "   SELECT "
      str_sql += "    SUM(user_bytes)/1024/1024 AS Kbytes_alloc "
      str_sql += "   ,tablespace_name "
      str_sql += "   ,COUNT(*) AS data_files "
      str_sql += "   FROM "
      str_sql += "   sys.dba_data_files "
      str_sql += "   GROUP BY "
      str_sql += "   tablespace_name "
      str_sql += ") b "
      str_sql += "ON "
      str_sql += "a.tablespace_name = b.tablespace_name "
      str_sql += "WHERE "
      str_sql += "b.tablespace_name = :p01 "
      str_sql += "ORDER BY 1 "

      curs = ez_execute(
          p_orcl
         ,str_sql
         ,{'p01':p_tablespace}
         ,p_tracing = p_tracing
      );

      row = curs.fetchone();

      return dba_free_format(row,p_format);

   else:

      str_sql  = "SELECT "
      str_sql += " 'Entire Instance' AS name "
      str_sql += ",SUM(kbytes_alloc)/1024 AS Allocated_Gig "
      str_sql += ",SUM(kbytes_alloc - NVL(kbytes_free,0))/1024 AS Used_Gig "
      str_sql += ",SUM(NVL(kbytes_free,0))/1024 AS Free_Gig "
      str_sql += ",( (SUM(kbytes_alloc - NVL(kbytes_free,0))/1024) / (SUM(kbytes_alloc)/1024) ) AS Percent_Used "
      str_sql += "FROM ( "
      str_sql += "   SELECT "
      str_sql += "    SUM(bytes)/1024/1024 AS Kbytes_free "
      str_sql += "   ,MAX(bytes)/1024/1024 AS largest "
      str_sql += "   ,tablespace_name "
      str_sql += "   FROM "
      str_sql += "   sys.dba_free_space "
      str_sql += "   GROUP BY "
      str_sql += "   tablespace_name "
      str_sql += ") a, ( "
      str_sql += "   SELECT "
      str_sql += "    SUM(user_bytes)/1024/1024 AS Kbytes_alloc "
      str_sql += "   ,tablespace_name "
      str_sql += "   ,COUNT(*) AS data_files "
      str_sql += "   FROM "
      str_sql += "   sys.dba_data_files "
      str_sql += "   GROUP BY "
      str_sql += "   tablespace_name "
      str_sql += ") b "
      str_sql += "WHERE "
      str_sql += "a.tablespace_name (+) = b.tablespace_name "

      curs = ez_execute(
          p_orcl
         ,str_sql
         ,p_tracing = p_tracing
      );

      row = curs.fetchone();

      return dba_free_format(row,p_format);
      
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def column_has_domain_index(
    p_orcl
   ,p_table_name
   ,p_column_name
   ,p_tracing = None
):

   schema,table_name = split2schematable(p_orcl,p_table_name);

   str_sql  = "SELECT "
   str_sql += " a.owner "
   str_sql += ",a.index_name "
   str_sql += "FROM "
   str_sql += "all_indexes a "
   str_sql += "JOIN "
   str_sql += "all_ind_columns b "
   str_sql += "ON "
   str_sql += "    a.owner       = b.index_owner "
   str_sql += "AND a.index_name  = b.index_name "
   str_sql += "WHERE "
   str_sql += "    a.table_owner = :p01 "
   str_sql += "AND a.table_name  = :p02 "
   str_sql += "AND b.column_name = :p03 "
   str_sql += "AND a.index_type  = 'DOMAIN' "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':schema,'p02':table_name,'p03':p_column_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      return None;
   else:
      return str(row[0]) + "." + str(row[1])
      
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def is_complex_object_table(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):
   schema,table_name = split2schematable(p_orcl,p_table_name);

   str_sql  = "SELECT "
   str_sql += "b.data_type "
   str_sql += "FROM "
   str_sql += "all_tables a "
   str_sql += "JOIN "
   str_sql += "( "
   str_sql += "   SELECT DISTINCT "
   str_sql += "    bb.owner "
   str_sql += "   ,bb.table_name "
   str_sql += "   ,bb.data_type "
   str_sql += "   FROM "
   str_sql += "   all_tab_columns bb "
   str_sql += "   WHERE "
   str_sql += "       bb.owner = :p01 "
   str_sql += "   AND bb.table_name = :p02 "
   str_sql += "   AND bb.data_type IN ('SDO_GEORASTER') "
   str_sql += ") b "
   str_sql += "ON "
   str_sql += "    a.owner = b.owner "
   str_sql += "AND a.table_name = b.table_name "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':schema,'p02':table_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      return None;
   else:
      return str(row[0])
      
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#

def get_sdo_georaster_tables(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):

   schema,table_name = split2schematable(p_orcl,p_table_name);
   column_name = None;
   output = [];

   # First collect the SDO_GEORASTER column, I guess we can assume there is only one?
   str_sql  = "SELECT "
   str_sql += "b.column_name "
   str_sql += "FROM "
   str_sql += "all_tables a "
   str_sql += "JOIN "
   str_sql += "( "
   str_sql += "   SELECT DISTINCT "
   str_sql += "    bb.owner "
   str_sql += "   ,bb.table_name "
   str_sql += "   ,bb.data_type "
   str_sql += "   ,bb.column_name "
   str_sql += "   FROM "
   str_sql += "   all_tab_columns bb "
   str_sql += "   WHERE "
   str_sql += "       bb.owner = :p01 "
   str_sql += "   AND bb.table_name = :p02 "
   str_sql += "   AND bb.data_type IN ('SDO_GEORASTER') "
   str_sql += ") b "
   str_sql += "ON "
   str_sql += "    a.owner = b.owner "
   str_sql += "AND a.table_name = b.table_name "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':schema,'p02':table_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();

   if row is None:
      raise Exception("ERROR cannot find SDO_GEORASTER column in " + str(p_table_name) + "!\n");
   else:
      column_name = row[0];

   # Second get the child table names out of the objects
   str_sql  = "SELECT "
   str_sql += "a.\"" + str(column_name) + "\".RASTERDATATABLE a "
   str_sql += "FROM "
   str_sql += "\"" + str(p_table_name) + "\" a "

   curs = ez_execute(
       p_orcl
      ,str_sql
      ,p_tracing = p_tracing
   );

   for table_name, in curs.fetchall():
      output.append(schema + '.' + table_name);

   curs.close();

   return output;
   
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   
def domain_index_status(
    p_orcl
   ,p_table_name
   ,p_tracing = None
):
   schema,table_name = split2schematable(p_orcl,p_table_name);
   
   str_sql  = "SELECT ";
   str_sql += "a.index_name ";
   str_sql += "FROM ";
   str_sql += "user_indexes a ";
   str_sql += "WHERE ";
   str_sql += "a.index_type = 'DOMAIN' AND ";
   str_sql += "a.table_name = :p01 ";
   
   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':table_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();                  
      
   if row is None:
      return None;
   
   index_name = row[0];
   
   str_sql  = "SELECT ";
   str_sql += "a.index_name ";
   str_sql += "FROM ";
   str_sql += "user_indexes a ";
   str_sql += "WHERE ";
   str_sql += "    a.index_name      = :p01 "
   str_sql += "AND a.index_type      = 'DOMAIN' ";
   str_sql += "AND a.status          = 'VALID' ";
   str_sql += "AND a.domidx_status   = 'VALID' "
   str_sql += "AND a.domidx_opstatus = 'VALID' " 
   
   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':index_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();           

   if row is None:
      return 'INVALID';
      
   str_sql  = "SELECT ";
   str_sql += "a.object_name ";
   str_sql += "FROM ";
   str_sql += "user_objects a ";
   str_sql += "WHERE ";
   str_sql += "    a.object_name = :p01 "
   str_sql += "AND a.object_type = 'INDEX' ";
   str_sql += "AND a.status      = 'VALID' " 
   
   curs = ez_execute(
       p_orcl
      ,str_sql
      ,{'p01':index_name}
      ,p_tracing = p_tracing
   );

   row = curs.fetchone();           

   if row is None:
      return 'INVALID';
   else:
      return 'VALID';
   
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
   #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$#
 