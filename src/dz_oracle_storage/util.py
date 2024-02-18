import os,sys;
import unicodedata,re;
    
############################################################################### 
def slugify(
    value        : str
   ,allow_unicode: bool = False
) -> str:
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

############################################################################### 
def dzx(pin):
   
   if pin is None or len(pin) == 0:
      return None;
   if isinstance(pin,str):
      return pin;
   else:
      return ",".join(f'\'{w}\'' for w in sorted(set(pin)));
   
############################################################################### 
def get_env_data(
   path: str
) -> dict:

   rez = {};
   with open(path, 'r') as f:
      for line in f.readlines():
         line = line.replace('\n','');
         if '=' in line and not line.startswith('#'):
            a,b = line.split('=');
            rez[a] = b;
            
   return rez;

###############################################################################
def read_spatial_parms(
    parms: str
) -> {}:

   rez = {};
   if parms is None:
      return rez;
      
   parms1 = parms.strip();
   if parms1 == "":
      return rez;
      
   parms1 = parms1.upper().strip();  
   parms1 = parms1.replace('  ',' ');
   parms1 = parms1.replace(' , ',' ');
   parms1 = parms1.replace(', ',' ');
   parms1 = parms1.replace(' = ','=');
   parms1 = parms1.replace(' =','=');
   parms1 = parms1.replace('= ','=');

   ary  = parms1.split(' ');
   for item in ary:
      k,v = item.split('=');
      rez[k] = v;
         
   return rez;
   
###############################################################################
def write_spatial_parms(
    parm_hash   : dict
   ,inject_parms: dict = None
) -> str:

   rez = "";
   if parm_hash is None or len(parm_hash) == 0:
      if inject_parms is None or len(inject_parms) == 0:
         return None;
      
      else:
         parm_hash = inject_parms
   
   else:
      if inject_parms is not None and len(inject_parms) > 0:
         for k,v in inject_parms.items():
            parm_hash[k.upper()] = v.upper();

   for k,v in parm_hash.items():
      rez += k + '=' + v + ' ';
      
   return rez.strip();
   
###############################################################################
def spatial_parms(
    parms       : str
   ,inject_parms: dict = None
) -> str:

   parm_hash = read_spatial_parms(
      parms = parms
   );
   
   if parm_hash is None or parm_hash == {}:
      return "";
   
   rez = write_spatial_parms(
       parm_hash    = parm_hash
      ,inject_parms = inject_parms
   );
   
   if rez is None or rez == "":
      return "";
      
   else:
      return "PARAMETERS(\'" + rez + "\')"; 

   
###############################################################################
def dzq(
    pin    : str
) -> str:

   if pin is None or pin == "":
      return None;
      
   pin1 = pin.strip();
      
   if pin1.startswith("\""):
      pin2 = pin1.replace("\"","");
      
      if pin2 == pin2.upper():
         return pin2; 
      else:
         return pin1;
      
   if pin1 == pin1.upper():
      return pin1;
      
   return "\"" + pin1 + "\"";
