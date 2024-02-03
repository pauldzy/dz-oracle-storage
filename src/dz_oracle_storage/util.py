import os,sys;
import unicodedata,re;
    
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

############################################################################### 
def dzx(pin):
   
   if pin is None or len(pin) == 0:
      return None;
   if isinstance(pin,str):
      return pin;
   else:
      return ",".join(f'\'{w}\'' for w in sorted(set(pin)));
   
############################################################################### 
def get_env_data(path: str) -> dict:
   rez = {};
   with open(path, 'r') as f:
      for line in f.readlines():
         line = line.replace('\n','');
         if '=' in line and not line.startswith('#'):
            a,b = line.split('=');
            rez[a] = b;
      return rez;
