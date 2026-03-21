import sys 
import os 
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__))) 

from modules.defense.loaders.arms_loader import load_all_arms_exports 
from modules.defense.cleaner import clean_arms_data 
from modules.defense.inserter import insert_arms_exports, verify_arms_insert 

print("Loading arms data...") 
df = load_all_arms_exports() 

print("Cleaning...") 
df = clean_arms_data(df) 

print("Verifying 'Total' is gone:") 
print(df[df['country'] == 'Total'])  # should print empty dataframe 

print("Inserting...") 
insert_arms_exports(df) 

print("Verifying...") 
verify_arms_insert()
