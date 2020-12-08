import os
import pandas as pd
import datetime
from param import *

#------------------------- Path --------------------------------------------
path= basepath+"Target"
ConPath = path+"/Consolidated"
#------------------------ Calling folder -----------------------------------
set_folder = os.listdir(path)
Active_con = []
for folder in set_folder:
    set_file = path+"/"+folder
    for file in os.listdir(set_file):
        if file.split("_")[0] =='A':
            Active_con.append(pd.read_csv(set_file+"/"+file))
df = pd.concat(Active_con, axis=0, sort=False)
df.to_csv(ConPath+"/"+"Active_KMP_AccessDB.csv", index=False)
print("Consolidation file generated successfully")