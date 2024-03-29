import os,sys;
from .ddl   import DDL;
from .util  import spatial_parms,dzq;

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
      return dzq(self._index_owner);
 
   @property
   def index_name(self):
      return dzq(self._index_name);
      
   @property
   def index_type(self):
      return self._index_type;
      
   @property
   def table_owner(self):
      return dzq(self._table_owner);
      
   @property
   def table_name(self):
      return dzq(self._table_name);
      
   @property
   def index_parameters(self):
      return self._index_parameters;
      
   @property
   def ityp_owner(self):
      return dzq(self._ityp_owner);
      
   @property
   def ityp_name(self):
      return dzq(self._ityp_name);
      
   @property
   def index_columns(self):
      return self._index_columns;
      
   ####
   def rebuild(
       self
      ,rebuild_spatial: bool = False
      ,set_compression: str  = None
      ,move_tablespace: str  = None
      ,priority_num   : int  = None
   ) -> list[DDL]:
   
      rez = [];

      if self.index_type == 'DOMAIN':
 
         if self.ityp_owner == 'MDSYS' and self.ityp_name in ['SPATIAL_INDEX','SPATIAL_INDEX_V2']:
            
            boo_move = False;
            if move_tablespace is None:
               tbs = {};
               
            else:
               tbs = {'TABLESPACE':move_tablespace};
               boo_move = True;
            
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
               rez.append(DDL(
                   priority_num    = priority_num
                  ,owner           = self.index_owner
                  ,segment_name    = self.index_name
                  ,partition_name  = None
                  ,segment_type    = 'INDEX'
                  ,ddl_rebuild     = False
                  ,ddl_move        = boo_move
                  ,ddl_recreate    = True
                  ,statements      = [
                      'DROP INDEX ' + self.index_owner + '.' + self.index_name + ';'
                     ,'CREATE INDEX ' + self.index_owner + '.' + self.index_name + ' ' \
                        + 'ON ' + self.table_owner + '.' + self.table_name             \
                        + '(' + self.index_columns + ') '                              \
                        + 'INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2 '  + prms + ';'
                  ]
               ));
            
            else:
               rez.append(DDL(
                   priority_num    = priority_num
                  ,owner           = self.index_owner
                  ,segment_name    = self.index_name
                  ,partition_name  = None
                  ,segment_type    = 'INDEX'
                  ,ddl_rebuild     = True
                  ,ddl_move        = boo_move
                  ,ddl_recreate    = False
                  ,statements      = ['ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD ' + prms + ';']
               ));
         
         else:
            rez.append(DDL(
                priority_num    = priority_num
               ,owner           = self.index_owner
               ,segment_name    = self.index_name
               ,partition_name  = None
               ,segment_type    = 'INDEX'
               ,ddl_rebuild     = False
               ,ddl_move        = False
               ,ddl_recreate    = False
               ,statements      = ['/* UNHANDLED DOMAIN INDEX ' + str(self.ityp_owner) + '.' + str(self.ityp_name) + ' */']
            ));
            
      elif self.index_type == 'LOB':
         None;
         
      elif self.index_type == 'IOT - TOP':
         None;
         
      elif self.index_type == 'BITMAP':
         sufx = "";
         
         if move_tablespace is not None:
            sufx += ' TABLESPACE ' + move_tablespace;
         
         rez.append(DDL(
             priority_num    = priority_num
            ,owner           = self.index_owner
            ,segment_name    = self.index_name
            ,partition_name  = None
            ,segment_type    = 'INDEX'
            ,ddl_rebuild     = False
            ,ddl_move        = False
            ,ddl_recreate    = False
            ,statements      = ['ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD' + sufx + ';']
         ));
         
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
                  
         rez.append(DDL(
             priority_num    = priority_num
            ,owner           = self.index_owner
            ,segment_name    = self.index_name
            ,partition_name  = None
            ,segment_type    = 'INDEX'
            ,ddl_rebuild     = False
            ,ddl_move        = False
            ,ddl_recreate    = False
            ,statements      = ['ALTER INDEX ' + self.index_owner + '.' + self.index_name + ' REBUILD' + sufx + ';']
         ));

      return rez;
      