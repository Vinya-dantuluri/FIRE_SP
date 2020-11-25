#Importing Packages
import pandas as pd
import re
import os.path
from time import strptime
from datetime import datetime, timedelta
import logging


#log file#
path1 = r"D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP"
if os.path.isfile("info.log"):
    os.remove(r"D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\info.log")
    logging.basicConfig(filename=r"D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\info.log", format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
else:
    logging.basicConfig(filename=r"D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\info.log", format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
    
    
#Reading source files#
try:
    logger.info("File are Processing.....")
    #Access DB Importing Data
    ADB_data = pd.read_csv(r'D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\Source\Access DB Secure.csv',encoding='latin1')
    #KMP Importing Data
    KMP_data = pd.ExcelFile(r'D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\Source\KMP Data.xlsx')
    #Metadata
    Metadata = pd.read_excel(r'D:\FSP\Output.xlsx')
    #Vista
    VISTA_data=pd.ExcelFile(r'D:\FSP\FPS Client 0211\FPS Client\Source Files\vistafile.xlsx')
    logger.info("All the files Processed...")
except:
    logger.error("Error occured while proccessing the Files....")
    

#Active Data
data_Active = ADB_data[(ADB_data["IsActive"] == True)]

#In-Active Data
data_InActive = ADB_data[ADB_data["IsActive"] != True]


def Mapping(df,dic):
    new_var =[]
#     print(df['Customer'])
    for i in df['Customer']:
#         print(i)
        if i in dic.keys():
            new_var.append(dic[i])
        else:
            new_var.append('NA')
    return new_var

def unMatched(df,dic):
    unmatched =[]
#     print(df['Customer'])
    for i in df['Customer']:
#         print(i)
        if i in dic:
#             print(i)
            unmatched.append('NA')
        else:
            unmatched.append(i)
    return unmatched



#Gobal declaration for category
Cat_L = [{1:['FP','HYD','BF','BFP']},{2:['FA','CO','HD','SD','DS','LSA','MPS','EOL']},{3:['FE','ELU','FHC','FLCS','EL']}]



#Effective date for AccessDB#

def effectivedate(data_Active1):
    try:
        logger.info("Processing of Original Date,Effective Date columns started!!!")
        list1=['JANUARY','APR','APRIL','AUGUST','DECEMBER','FEBRUARY','JULY','JUNE','MARCH','MAY','NOVEMBER','OCTOBER','SEPTEMBER']
        data_Active1 = data_Active1[(data_Active1['Inspection Month'].str.len() > 0) &(data_Active1['Inspection Month'].isin(list1))]
        data_Active1['Inspection Quote2 #']= pd.to_datetime(data_Active1['Inspection Quote #'], errors='coerce').dt.strftime('%Y-%d-%b')
        data_Active1['new']=''
        x=[]
        for i in range(0,len(data_Active1['Inspection Quote2 #'])):
            if type(data_Active1['Inspection Quote2 #'].iloc[i])!=float:
                data_Active1['Inspection Quote #'].iloc[i] = data_Active1['Inspection Quote2 #'].iloc[i]
        data_Active1['Inspection Quote_new']=data_Active1['Inspection Quote #']
        data_Active1['Inspection Quote #'] = data_Active1['Inspection Quote #'].str.split('-').str[0]
        data_Active1 = data_Active1[(data_Active1['Inspection Quote #'].str.len() > 0)]
        try:
            logger.info("Processing of Original Date column started!!!")
            for i in range(0,len(data_Active1)):  
                if data_Active1['Inspection Quote #'].iloc[i].isnumeric():
                    x.append(data_Active1[['Inspection Quote #','Inspection Month','Inspection Quote_new']].iloc[i])
                else:
                    data_Active1['new'].iloc[i]=data_Active1['Inspection Quote #'].iloc[i][-4:]
                    com=data_Active1['new'].iloc[i]
                    if com.isnumeric() and int(com)>2000:
                        data_Active1['Inspection Quote #'].iloc[i]=com
                        x.append(data_Active1[['Inspection Quote #','Inspection Month','Inspection Quote_new']].iloc[i])
                    else:
                        data_Active1[['Inspection Quote #','Inspection Month','Inspection Quote_new']].iloc[i]==''
            df = pd.DataFrame(x)
            df = df['Inspection Month']+'/'+'01'+'/'+df['Inspection Quote #']
            df= pd.to_datetime(df, errors='coerce').dt.strftime('%m/%d/%Y')
            result = df
            logger.info("processing of Original Date column is successful!!!")
        except: 
            logger.info("Error occured while processing of Original Date column started!!!")
        
        
        df_date  = pd.to_datetime(result)
        df_date['date'] = pd.to_datetime(df_date)
 
        try:
            logger.info("Processing of Effective Date started!!!")
            for i in range(0,len(result)):
                df_date["date"].iloc[i] = "01/01/2021"
            df_date["date"] = pd.to_datetime(df_date["date"], errors='coerce').dt.strftime('%m/%d/%Y')
            result1= df_date["date"]
            logger.info("Error occured while processing of Effective Date is successful!!!")
        except:
            logger.info("Processing of Effective Date started!!!")
        return  (result,result1)
        logger.info("processing of Original Date, Effective Date is successful!!!")
    except:
        logger.info("Error occured while processing of Original Date, Effective Date started!!!")
        


#Acronyms#
def MapDesc(data_Active):
    try:
        logger.info("Processing of Customer Descrption is started!!!")
        temp = data_Active['Description'].str.split(' ')
        temp = temp.fillna(0)
        var = []
        
        for i in temp:
            if i != 0:
                temp = ''
                for j in i:
                    cnt = 0
                    for k in j:
                        if k.isupper():
                            cnt+=1
                    if cnt > 1:
                        temp = temp + j + "|"
                var.append(temp[:-1])
            else:
                var.append('')
        
        data_Active['desc'] = var
        data_Active['desc'] = data_Active['desc'].str.replace(',', '')
        data_Active['desc'] = data_Active['desc'].str.replace(')', '')
        data_Active['desc'] = data_Active['desc'].str.replace('(', '')
        data_Active['desc'] = data_Active['desc'].str.replace("'s", '')
        var = data_Active['desc']

        fnl = []
        temp2 = []
        temp1 = ''
        for i in var:
            temp2=[]
            for j in range(0,len(Cat_L)):
                temp1 = ''
                for k in Cat_L[j][j+1]:
                    for l in i.split("|"):
                            if k == l:
                                temp1 = temp1 + str(l) + ","
                if temp1 != '':
                    temp2.append(temp1[:-1])
                else:
                    temp2.append(1)
            cnt = 0
            for n in range(0,len(Cat_L)):
                if temp2[n] == 1:
                    cnt +=1
            if cnt != len(Cat_L):
                fnl.append(temp2)
            else:
                fnl.append(0)

        data_Active['C_list'] = fnl
        KMP_CL_F= (data_Active
                 .set_index(['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Description','Effective Date'])['C_list']
                 .apply(pd.Series)
                 .stack()
                 .reset_index()
                 #.drop('level_4', axis=1)
                 .rename(columns={0:'Desc'}))
        KMP_CL_F = KMP_CL_F[KMP_CL_F['Desc'] !=1].reset_index()
        for i in range(0,len(KMP_CL_F['Desc'])):
            if KMP_CL_F['Desc'][i] == 0:
                KMP_CL_F['Desc'][i] = KMP_CL_F['Description'][i]
        KMP_CL_F['Desc'] = KMP_CL_F['Desc'].fillna('NA')
        KMP_CL_F['Customer'] = KMP_CL_F['Customer'].apply(str)
        KMP_CL_F['Cus-Desc'] = KMP_CL_F['Customer'] + "-" + KMP_CL_F['Desc']
        return KMP_CL_F
        logger.info("Customer Descrption processing is successful!!!")
    except:
        logger.info("error ocured while processing of Customer Descrption is started!!!")


#Active Export#
try:
    logger.info("Exporting of Active AccessDB file Started!!")
    output = pd.DataFrame(columns=list(Metadata['Output File1']))
    output[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Description']] = data_Active[['Inspection Quote #','Inspection Type','Legal Company Name','Price','PO#','Site Address','Fire Protection Equipment']]
    output['Agreement Price'] = output['Agreement Price'].fillna(0)
    DateColumns=list(Metadata['Output File1'])
    VISTA =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
    CT_ED_Dic = dict(zip(VISTA['Name'], VISTA['Customer']))
    output['Customer1'] = Mapping(output,CT_ED_Dic)
    #output['UnMatched']= unMatched(output,CT_ED_Dic)
    output['Customer']=output['Customer1']
    temp = MapDesc(output)
    output1 = pd.DataFrame(columns=list(Metadata['Output File1']))
    output1[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Description','Effective Date']] = temp[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Cus-Desc','Effective Date']]
    for i in DateColumns:
        if str(i).find('Date') != -1:
            output1[i]= output1[i].fillna('01/01/2020')
            output1[i]=  pd.to_datetime(output1[i], errors='coerce').dt.strftime('%m/%d/%Y')
        elif str(i).find('Pricing') != -1:
            output1[i]=output1['Pricing'].fillna('0')
    output1['Original Date'],output1['Effective Date'] = effectivedate(data_Active)
    output1.to_csv(r'D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\Target\Access\Active_AccessDB.csv', index = False)
    logger.info("Exporting of Active AccessDB file is successfull!!")
except:
    logger.info("Error while exporting of Active AccessDB file Started!!")


#In-Active Data
try:
    logger.info("Exporting of Active AccessDB file Started!!")
    output = pd.DataFrame(columns=list(Metadata['Output File1']))
    output[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Service Site','Description']] = data_InActive[['Inspection Quote #','Inspection Type','Legal Company Name','Price','Site Address','Fire Protection Equipment']]
    output['Agreement Price'] = output['Agreement Price'].fillna(0)
    DateColumns=list(Metadata['Output File1'])

    VISTA =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
    CT_ED_Dic = dict(zip(VISTA['Name'], VISTA['Customer']))
    output['Customer1'] = Mapping(output,CT_ED_Dic)
    output['Customer']=output['Customer1']

    temp = MapDesc(output)

    output1 = pd.DataFrame(columns=list(Metadata['Output File1']))
    output1[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Description','Effective Date']] = temp[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Cus-Desc','Effective Date']]
    for i in DateColumns:
        if str(i).find('Date') != -1:
            output1[i]= output1[i].fillna('01/01/2020')
            output1[i]=  pd.to_datetime(output1[i], errors='coerce').dt.strftime('%m/%d/%Y')
        elif str(i).find('Pricing') != -1:
            output1[i]=output1['Pricing'].fillna('0') 
    output1['Original Date'],output1['Effective Date'] = effectivedate(data_InActive)
    output1.to_csv(r'D:\FSP\FPS Client 0211\FPS Client\Project_FIRE-SP\Target\Access\In_Active_AccessDB.csv', index = False)
    logger.info("Exporting of Active AccessDB file is successfull!!")
except:
    logger.info("Error while exporting of Active AccessDB file Started!!")
