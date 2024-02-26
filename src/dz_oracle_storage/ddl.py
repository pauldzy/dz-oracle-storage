import os,sys;

############################################################################### 
class DDL(object):

   def __init__(
       self
      ,priority_num    : int
      ,owner           : str
      ,segment_name    : str
      ,partition_name  : str
      ,segment_type    : str
      ,ddl_rebuild     : bool
      ,ddl_move        : bool
      ,ddl_recreate    : bool
      ,statements      : list
   ):
   
      self._priority_num     = priority_num;
      self._owner            = owner;
      self._segment_name     = segment_name;
      self._partition_name   = partition_name;
      self._segment_type     = segment_type;
      self._ddl_rebuild      = ddl_rebuild;
      self._ddl_move         = ddl_move;
      self._ddl_recreate     = ddl_recreate;
      self._statements       = statements;
      
   @property
   def priority_num(self):
      return self._priority_num;
      
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
   def ddl_rebuild(self):
      return self._ddl_rebuild;
      
   @property
   def ddl_move(self):
      return self._ddl_move;
      
   @property
   def ddl_recreate(self):
      return self._ddl_recreate;
      
   @property
   def statements(self):
      return self._statements;
      
   ############################################################################
   def statements_str(
       self
      ,lb = False    
   ):
      rez = "";
      
      for item in self.statements:
         
         rez += item;
         
         if lb:
            rez += "\n";
         
      return rez;
      
   