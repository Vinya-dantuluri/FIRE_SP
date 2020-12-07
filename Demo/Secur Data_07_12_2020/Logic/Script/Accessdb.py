#Importing Packages
import pandas as pd
pd.options.mode.chained_assignment = None
import re
import os.path
from time import strptime
import time
from datetime import datetime, timedelta
import logging
from param import *


print("\n--> Access DB Process Started")
time.sleep(3)

#log file#
if os.path.isfile("info.log"):
    os.remove(AcessLog)
    logging.basicConfig(filename=AcessLog, format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
else:
    logging.basicConfig(filename=AcessLog, format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 

    
    
#Reading source files#
try:
    logger.info("File are Processing.....")
    #Access DB Importing Data
    ADB_data = pd.read_excel(ADB_data)
    print("\n--> Source file contains",len(ADB_data),"records")
    time.sleep(2)
    #Metadata
    Metadata = pd.read_excel(Metadata)
    #Vista
    VISTA_data=pd.ExcelFile(VISTA_data)
	#category file#
    FnR = pd.read_csv(FnR)
    logger.info("All the files Processed...")
    print("\n--> Source File Uploaded")
    time.sleep(2)
    #Active Data
    data_Active = ADB_data[(ADB_data["IsActive"] == True)]
    print("\n--> Total Active records from source file are",len(data_Active))
    time.sleep(2)
    #In-Active Data
    data_InActive = ADB_data[ADB_data["IsActive"] != True]
    print("\n--> Total InActive records from source file are",len(data_InActive))
    time.sleep(2)

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
    Cat_L = [{1:['FP','HYD','BF','BFP','SPR','WET','DRY','TS','PRV','GLYCOL','STANDPIPE','BOOSTER','PREACTION','CURB_BOX']},
            {2:['FA','CO','HD','SD','DS','LSA','MPS','EOL','FACP','SA','VOICE']},
            {3:['FE','EL','FHC','FLCS','EXT','EXIT']},
            {4:['SERVICES','INTRUSION','BURGULAR','EQUIPMENT','FIRE','ELEVATOR','FACP']}
            ]
    Cat_K = {1:"Sprinkler",2:"Fire Alarm",3:"Route",4:"Monitoring"}


    #Effective date for AccessDB#

    def effectivedate(data_Active1):
        try:
            logger.info("Processing of Effective date1 Started!!!!!!")
            list1=['JULY/AUGUST','NOVEMBER/DECEMBER']
            list2=['JANUARY','APRIL','AUGUST','DECEMBER','FEBRUARY','JULY','JUNE','MARCH','MAY','NOVEMBER','OCTOBER','SEPTEMBER']
            list3=['ANNUAL','MONTHLY','BI-MONTHLY','QUARTELY','WEEKLY','MONTHLY & MONITORING','MONTHLY QI ONLY','MONTHLY/QTLY','MONTHLY/QI','MONTHLY/WKLY']
            list4=['Tender OCHRFQ17-001','Tender EJ196-190631','Tender EJ196-19063','BS Project P1239','Project #1208']
            list5=['Tender-16-1257','Tender-19-1139','Tender-19-1140','AllardQ-2018','Q1535-1541Main2019']
            data_Active1 = data_Active1[(data_Active1['Inspection Month'].str.len() > 0) & ~(data_Active1['Inspection Month'].isin(list1))]
            data_Active1 = data_Active1[~(data_Active1['Inspection Quote #'].isin(list4))]
            data_Active1['Inspection Quote2 #']= pd.to_datetime(data_Active1['Inspection Quote #'], errors='coerce').dt.strftime('%Y-%d-%b')
            data_Active1['new']=''
            x=[]

            for i in range(0,len(data_Active1['Inspection Quote2 #'])):
                if type(data_Active1['Inspection Quote2 #'].iloc[i])!=float:
                    data_Active1['Inspection Quote #'].iloc[i] = data_Active1['Inspection Quote2 #'].iloc[i]

            data_Active1['Inspection Quote_new']=data_Active1['Inspection Quote #']
            data_Active1 = data_Active1[(data_Active1['Inspection Quote #'].str.len() > 0)]

            for i in range(0,len(data_Active1['Inspection Quote #'])):
                if data_Active1['Inspection Quote #'].iloc[i] in list5:
                    data_Active1['Inspection Quote #'].iloc[i] = data_Active1['Inspection Quote #'].iloc[i].split('-')[1]
                    data_Active1['Inspection Quote #'].iloc[i]=data_Active1['Inspection Quote #'].iloc[i][-2:]

                elif data_Active1['Inspection Quote #'].iloc[i]=="See 2019 invoice":

                    data_Active1['Inspection Quote #'].iloc[i] = data_Active1['Inspection Quote #'].iloc[i].split(' ')[1]
        #             print(data_Active1['Inspection Quote #'].iloc[i])
                else:
                    data_Active1['Inspection Quote #'].iloc[i] = data_Active1['Inspection Quote #'].iloc[i].split('-')[0]

            data_Active1['Inspection Quote #'] = data_Active1['Inspection Quote #'].str[-2:]
            df = pd.DataFrame()
            df = data_Active1
            df['my Month']=''

            for i in range(0,len(df)):
                if df['Inspection Month'].iloc[i] in list2:
                    df['my Month'].iloc[i] = df['Inspection Month'].iloc[i]+'/'+'01'+'/'+df['Inspection Quote #'].iloc[i]
                elif df['Inspection Month'].iloc[i] in list3:
                    df['my Month'].iloc[i] = 'JANUARY'+'/'+'01'+'/'+df['Inspection Quote #'].iloc[i]
            df['my Month'] = pd.to_datetime(df['my Month'], errors='coerce').dt.strftime('%m/%d/%Y')
            result = df['my Month']
            try:
                logger.info("Processing of Effective date started!!!")

                df_date  = pd.to_datetime(result)
                df_date['date'] = pd.to_datetime(df_date)

                df_date["date"] = df_date["date"].map(lambda x: x.replace(year=2021))
                df_date["date"] = pd.to_datetime(df_date["date"], errors='coerce').dt.strftime('%m/%d/%Y')
                result1= df_date["date"]

                return  (result,result1)
                logger.info("Succesfully generated Effective date and Effective date1")
            except Exception as e:
                print("\nError :->" + str(e) +"\n")
                logger.error("Error occured while processing of Effective date %s" %e)
        except Exception as e:
            print("\nError :->" + str(e) +"\n")
            logger.error("Error occured while processing of Effective date1!!!!!! %s" %e)

            
    #Acronyms#
    def MapDesc(data_Active):
        data_Active['Description'] = data_Active['Description'].fillna(0)
        fnl = []
        temp2 = []
        temp1 = ''
        
        for i in data_Active['Description']:
            if i != 0:
                temp2=[]
                for j in range(0,len(Cat_L)):
                    temp1 = ''
                    for k in Cat_L[j][j+1]:
                        flag = 0
                        for l in str(i).split(' '):
                            if k == l.strip():
                                if Cat_K[j+1] not in temp1:
                                    temp1 = temp1 + Cat_K[j+1] +"-"+ str(l) + ","
                                else:
                                    if str(l) not in temp1:
                                        temp1 = temp1 + str(l) + ","
                    if temp1 != '':
                        temp2.append(temp1[:-1])
                    else:
                        temp2.append(1)
                cnt = 0
                for n in range(0,len(Cat_L)):
                    if temp2[n] ==1:
                        cnt +=1
                if cnt != len(Cat_L):
                    fnl.append(temp2)
                else:
                    fnl.append(0)
            else:
                fnl.append(0)
                
        data_Active['descNew'] = fnl
        #data_Active[['Description','descNew','Alt Agreement','Customer2']].to_csv(str(letter)+"_Mapping.csv")
        data_Active['C_list'] = fnl
        KMP_CL_F= (data_Active
                .set_index(['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Customer2','Agreement Price','Customer PO','Service Site','Description','Effective Date','Original Date','Description1'])['C_list']
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
        KMP_CL_F['Description1'] = KMP_CL_F['Desc'].str.split('-').str[1]
        KMP_CL_F['Customer2'] = KMP_CL_F['Customer2'].apply(str)
        KMP_CL_F['Cus-Desc'] = KMP_CL_F['Customer2'] + "-" + KMP_CL_F['Desc'].str.split('-').str[0]
        return KMP_CL_F
        
    #Data cleaning#
    def dataClean(df):
        var = []
        df['DescriptionNew'] = df['Description']
        for i in df['Description']:
            var.append((re.sub(r'[^a-zA-Z| ]',r'',str(i))).strip())
        df['Description'] = var
        for i in range(0,len(FnR)):
            df['Description'] = df['Description'].str.replace(FnR['Find'][i],FnR['Replace'][i])
        return df['Description']
        

    #Active Export#
    try:
        logger.info("Exporting of Active Access DB started!!!")
        output = pd.DataFrame(columns=list(Metadata['Output File1']))
        output[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site',
                'Description']] = data_Active[['Inspection Quote #','Inspection Type','Legal Company Name','Price','PO#',
                                            'Site Address','Fire Protection Equipment']]
        output=output[(output['Description'].str.contains('cancelled')==False) & (output['Description'].str.contains('Cancelled')== False)]
        output['Original Date'],output['Effective Date'] = effectivedate(data_Active)
        output['Agreement Price'] = output['Agreement Price'].fillna(0)
        DateColumns=list(Metadata['Output File1'])
        lis =['Original Date','Effective Date']
        VISTA =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
        CT_ED_Dic = dict(zip(VISTA['Name'], VISTA['Customer']))
        output['Description'] = dataClean(output)
        output['Customer1'] = Mapping(output,CT_ED_Dic)
        output['Customer2']= output['Customer']
        #output['UnMatched']= unMatched(output,CT_ED_Dic)
        output['Customer']=output['Customer1']
        output['Expiration Date'] = '01/01/2022'
        # temp = output
        temp = MapDesc(output)
        output1 = pd.DataFrame(columns=list(Metadata['Output File1']))
        output1[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site',
            'Description','Effective Date','Original Date','Description1']] = temp[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer',
                                                    'Agreement Price','Customer PO','Service Site','Cus-Desc','Effective Date','Original Date',
                                                                    'Description1']]
        for i in DateColumns:
            if (str(i).find('Date') != -1):
                if str(i) not in lis:
                    output1[i]= output1[i].fillna('01/01/2021')
                    output1[i]=  pd.to_datetime(output1[i], errors='coerce').dt.strftime('%m/%d/%Y')
            elif str(i).find('Pricing') != -1:
                output1[i]=output1['Pricing'].fillna('0')
        output1.to_csv(ActiveAccess, index = False)
        print("\n--> AccessDB Active File Generation Completed")
        time.sleep(2)
        logger.info("Successfully completed Active Access DB file")
    except Exception as e:
        print("\nError :->" + str(e) +"\n")
        logger.error("Error occurred while exporting Access DB started... %s" %e)

        


    #In-Active Data
    try:
        logger.info("Export InActive Access DB file started!!!!!")
        output = pd.DataFrame(columns=list(Metadata['Output File1']))
        output[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site','Description']] = data_InActive[['Inspection Quote #','Inspection Type','Legal Company Name','Price','PO#','Site Address','Fire Protection Equipment']]
        output=output[(output['Description'].str.contains('cancelled')==False) & (output['Description'].str.contains('Cancelled')== False)]
        output['Original Date'],output['Effective Date'] = effectivedate(data_InActive)
        output['Agreement Price'] = output['Agreement Price'].fillna(0)
        DateColumns=list(Metadata['Output File1'])
        lis =['Original Date','Effective Date']
        VISTA =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
        CT_ED_Dic = dict(zip(VISTA['Name'], VISTA['Customer']))
        output['Description'] = dataClean(output)
        output['Customer1'] = Mapping(output,CT_ED_Dic)
        output['Customer2']= output['Customer']
        #output['UnMatched']= unMatched(output,CT_ED_Dic)
        output['Customer']=output['Customer1']
        output['Expiration Date'] = '01/01/2022'
        # temp = output
        temp = MapDesc(output)
        output1 = pd.DataFrame(columns=list(Metadata['Output File1']))
        output1[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer','Agreement Price','Customer PO','Service Site',
            'Description','Effective Date','Original Date','Description1']] = temp[['Alt Agreement','Agreement Type '+'('+'Data Pull)','Customer',
                                                    'Agreement Price','Customer PO','Service Site','Cus-Desc','Effective Date','Original Date',
                                                                    'Description1']]
        for i in DateColumns:
            if (str(i).find('Date') != -1):
                if str(i) not in lis:
                    output1[i]= output1[i].fillna('01/01/2021')
                    output1[i]=  pd.to_datetime(output1[i], errors='coerce').dt.strftime('%m/%d/%Y')
            elif str(i).find('Pricing') != -1:
                output1[i]=output1['Pricing'].fillna('0')
        output1.to_csv(InactiveAccess, index = False)
        logger.info("Exporting of InActive AccessDB file is successfull!!")
        print("\n--> AccessDB InActive File Generation Completed")
        time.sleep(2)
    except Exception as e:
        print("\nError :->" + str(e) +"\n")
        logger.error("Error occurred while Exporting  InActive Access DB file!!!!!")



except Exception as e:
    print("\nError :->" + str(e) +"\n")
    logger.error("Error occured while proccessing the Files.... %s" %e)


