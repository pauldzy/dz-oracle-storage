import os,sys;

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
   ) -> list[str]:
   
      rez = [];
      curs = self._sqliteconn.cursor();      
      
      str_sql = """
         SELECT
          a.owner
         ,a.index_name
         ,a.index_type
         FROM
         dba_indexes a
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
      
      for row in curs:
         index_owner  = row[0];
         index_name   = row[1];
         index_type   = row[2];
         
         if index_type == 'DOMAIN':
            rez.append('/* domain index */');
            rez.append('ALTER INDEX ' + index_owner + '.' + index_name + ' REBUILD;');
            
         elif index_type == 'LOB':
            None;
            
         else:
            rez.append('ALTER INDEX ' + index_owner + '.' + index_name + ' REBUILD;');

      curs.close();
      
      return rez;
      