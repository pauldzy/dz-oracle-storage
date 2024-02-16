import os,sys;
from .util import spatial_parms;

############################################################################### 
class Table(object):

   def __init__(
       self
      ,parent
      ,table_owner
      ,table_name
   ):
   
      self._parent           = parent;
      self._sqliteconn       = parent._sqliteconn;
      self._table_owner      = table_owner;
      self._table_name       = table_name;
      
   @property
   def table_owner(self):
      return self._table_owner;
 
   @property
   def table_name(self):
      return self._table_name;
      
   ####
   def rebuild_indexes(
       self
      ,rebuild_spatial: bool = False
   ) -> list[str]:
   
      rez = [];
      curs = self._sqliteconn.cursor();      
      
      str_sql = """
         SELECT
          a.owner
         ,a.index_name
         ,a.index_type
         ,a.table_owner
         ,a.table_name
         ,a.parameters
         ,a.ityp_owner
         ,a.ityp_name
         ,a.index_columns
         FROM
         dba_indexes_plus a
         WHERE
             a.table_owner = :p01
         AND a.table_name  = :p02
      """;
      
      curs.execute(
          str_sql
         ,{
             'p01': self.table_owner
            ,'p02': self.table_name
          }
      );
      
      index_owner      = None;
      index_name       = None;
      index_type       = None;
      table_owner      = None;
      table_name       = None;
      index_parameters = None;
      ityp_owner       = None;
      ityp_name        = None;
      index_columns    = None;
      for row in curs:
         index_owner      = row[0];
         index_name       = row[1];
         index_type       = row[2];
         table_owner      = row[3];
         table_name       = row[4];
         index_parameters = row[5];
         ityp_owner       = row[6];
         ityp_name        = row[7];
         index_columns    = row[8];
         
         if index_type == 'DOMAIN':
            
            if ityp_owner == 'MDSYS' and ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
               
               if rebuild_spatial:
                  prms = spatial_parms(
                     parms = index_parameters
                  );
                  rez.append('DROP INDEX ' + index_owner + '.' + index_name + ';');
                  rez.append('CREATE INDEX ' + index_owner + '.' + index_name + ' ' \
                     + 'ON ' + table_owner + '.' + table_name                       \
                     + '(' + index_columns + ') '                                   \
                     + 'INDEXTYPE IS "MDSYS"."SPATIAL_INDEX_V2" '  + prms + ';'); 
               
               else:
                  rez.append('ALTER INDEX ' + index_owner + '.' + index_name + ' REBUILD;');
            
            else:
               rez.append('/* UNHANDLED DOMAIN INDEX ' + str(ityp_owner) + '.' + str(ityp_name) + ' */');
               
         elif index_type == 'LOB':
            None;
            
         else:
            rez.append('ALTER INDEX ' + index_owner + '.' + index_name + ' REBUILD;');

      curs.close();
      
      return rez;
      