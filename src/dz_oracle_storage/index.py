import os,sys;
from .util import spatial_parms;

############################################################################### 
class Index(object):

   def __init__(
       self
      ,parent
      ,index_owner     : str
      ,index_name      : str
      ,index_type      : str = None
      ,table_owner     : str = None
      ,table_name      : str = None
      ,index_parameters: str = None
      ,ityp_owner      : str = None
      ,ityp_name       : str = None
      ,index_columns   : str = None
   ):
   
      self._parent           = parent;
      self._sqliteconn       = parent._sqliteconn;
      self._index_owner      = index_owner;
      self._index_name       = index_name;
      self._index_type       = index_type;
      self._table_owner      = table_owner;
      self._table_name       = table_name;
      self._index_parameters = index_parameters;
      self._ityp_owner       = ityp_owner;
      self._ityp_name        = ityp_name;
      self._index_columns    = index_columns;
      
      if index_type is None:
      
         curs = self._sqliteconn.cursor();      
         
         str_sql = """
            SELECT
             a.index_type
            ,a.table_owner
            ,a.table_name
            ,a.parameters
            ,a.ityp_owner
            ,a.ityp_name
            ,a.index_columns
            FROM
            dba_indexes_plus a
            WHERE
                a.owner       = :p01
            AND a.index_name  = :p02
         """;
         
         curs.execute(
             str_sql
            ,{
                'p01': self._index_owner
               ,'p02': self._index_name
             }
         );
         
         index_type       = None;
         table_owner      = None;
         table_name       = None;
         index_parameters = None;
         ityp_owner       = None;
         ityp_name        = None;
         index_columns    = None;
         for row in curs:
            index_type       = row[0];
            table_owner      = row[1];
            table_name       = row[2];
            index_parameters = row[3];
            ityp_owner       = row[4];
            ityp_name        = row[5];
            index_columns    = row[6];
            
         self._index_type       = index_type;
         self._table_owner      = table_owner;
         self._table_name       = table_name;
         self._index_parameters = index_parameters;
         self._ityp_owner       = ityp_owner;
         self._ityp_name        = ityp_name;
         self._index_columns    = index_columns;
      
   @property
   def index_owner(self):
      return self._index_owner;
 
   @property
   def index_name(self):
      return self._index_name;
      
   @property
   def index_type(self):
      return self._index_type;
      
   @property
   def table_owner(self):
      return self._table_owner;
      
   @property
   def table_name(self):
      return self._table_name;
      
   @property
   def index_parameters(self):
      return self._index_parameters;
      
   @property
   def ityp_owner(self):
      return self._ityp_owner;
      
   @property
   def ityp_name(self):
      return self._ityp_name;
      
   @property
   def index_columns(self):
      return self._index_columns;
      
   ####
   def rebuild(
       self
      ,rebuild_spatial: bool = False
      ,set_compression: str  = None
      ,move_tablespace: str  = None
   ) -> list[str]:
   
      rez = [];

      if self.index_type == 'DOMAIN':
 
         if self.ityp_owner == 'MDSYS' and self.ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
            
            if move_tablespace is None:
               tbs = {};
               
            else:
               tbs = {'TABLESPACE':move_tablespace};
            
            if set_compression is None:
               cmp = {};
               
            else:
               cmp = {'SECUREFILE':'TRUE'};
               
               if set_compression in ['NONE','OFF','NOCOMPRESS']:
                  cmp['COMPRESSION'] = 'OFF';               
               elif set_compression in ['LOW']:
                  cmp['COMPRESSION'] = 'LOW';
               elif set_compression in ['MEDIUM']:
                  cmp['COMPRESSION'] = 'MEDIUM';
               elif set_compression in ['HIGH']:
                  cmp['COMPRESSION'] = 'HIGH';
               else:
                  raise Exception('unknown compression value for spatial index.');
            
            prms = spatial_parms(
               parms        = self.index_parameters
              ,inject_parms = tbs | cmp
            );
            
            if rebuild_spatial:
               rez.append('DROP INDEX ' + self.index_owner + '.' + self.index_name + ';');
               rez.append('CREATE INDEX ' + self.index_owner + '.' + self.index_name + ' ' \
                  + 'ON ' + self.table_owner + '.' + self.table_name                       \
                  + '(' + self.index_columns + ') '                                        \
                  + 'INDEXTYPE IS "MDSYS"."SPATIAL_INDEX_V2" '  + prms + ';'); 
            
            else:
               rez.append('ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD ' + prms + ';');
         
         else:
            rez.append('/* UNHANDLED DOMAIN INDEX ' + str(self.ityp_owner) + '.' + str(self.ityp_name) + ' */');
            
      elif self.index_type == 'LOB':
         None;
         
      elif self.index_type == 'IOT - TOP':
         None;
         
      elif self.index_type == 'BITMAP':
         rez.append('ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD;');
         
      else:
         sufx = "";
         
         if move_tablespace is not None:
            sufx += ' TABLESPACE ' + move_tablespace;
         
         if set_compression is not None:
            if set_compression in ['NONE','OFF','NOCOMPRESS']:
               sufx += ' NOCOMPRESS';               
            elif set_compression in ['LOW','COMPRESS']:
               sufx += ' COMPRESS';
            elif set_compression in ['MEDIUM']:
               sufx += ' COMPRESS ADVANCED LOW';
            elif set_compression in ['HIGH']:
               sufx += ' COMPRESS ADVANCED HIGH';
            else:
               raise Exception('unknown compression index value.');
                  
         rez.append('ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD' + sufx + ';');

      return rez;
      