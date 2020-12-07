#Importing Packages#
import pandas as pd
from pandas import DataFrame
import re
import os.path
from time import strptime
import time
from datetime import datetime
import logging
from param import *

print("\n--> KMP Process Started")

#log file#
if os.path.isfile("info.log"):
    os.remove(KMPLog)
    logging.basicConfig(filename=KMPLog, format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
else:
    logging.basicConfig(filename=KMPLog, format='%(asctime)s %(message)s', filemode='w') 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
    
    
#Reading source files#
try:
    logger.info("File are Processing.....")
    #KMP Importing Data
    KMP_data = pd.ExcelFile(KMP_data)
    #print("\n--> Source file contains",len(KMP_data),"records")
    #Metadata
    Metadata = pd.read_excel(Metadata)
    #Vista
    VISTA_data=pd.ExcelFile(VISTA_data)
    logger.info("All the files Processed...")
    print("\n--> Source File Uploaded")
    time.sleep(2)

    ### IGNORE LIST IN KMP##
    ignore_list=pd.read_excel(ignore_list)
    ignore_list = ignore_list['IGNORE LIST'].values.tolist()

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

    ################## List of unmatched customer code from vista file ##################


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


    ##################  ##################
    def KMP_TraF(KMP_CL,KMP_CT):
        try:
            logger.info("Processing of Description,Pricing to 0 columns  started!!!")
            join=pd.merge(KMP_CL,KMP_CT,how='inner',left_on='ContractNumber',right_on='ContractNumber')
            KMP_CL=join.drop(columns={'ContractType','CustomerCode','Locations','ContractStatus','ContractEnd','ContractTerm','ContractValue','ContractNotes'})
            x=KMP_CL.rename(columns={'CustomerName_y': 'CustomerName'})
            output = pd.DataFrame(columns=list(Metadata['Output File1']))
            output['Description']=x['ContractDescription']+"-"+x['CustomerName']
            output[['Alt Agreement','Agreement Type (For Entry)','Agreement Type (Data Pull)','Customer','Service Site','Original Date']] = x[['ContractNumber','Alt','Address2','CustomerName','Address1','ContractStart']]
            output['Expiration Date']='01/01/2022'
            output['Effective Date']=output['Original Date']
            output['Effective Date']=output['Effective Date'].map(lambda x: x.replace(year=2021))
            DateColumns=list(Metadata['Output File1'])
            for i in DateColumns:
                if str(i).find('Date') != -1:
                    output[i]= output[i].fillna('01/' +'01/'+ '2021')
                    output[i]=  pd.to_datetime(output[i], errors='coerce').dt.strftime('%m/%d/%Y')
                elif str(i).find('Pricing') != -1:
                    output[i]=output['Pricing'].fillna('0')
            try:
                logger.info("Reading vista file for matching Customer code with KMP File Started!!")
                CT_ED_Dic = dict(zip(Vista['Name'].str.upper(), Vista['Customer']))
                output['Customer'] = Mapping(output,CT_ED_Dic)
                #output['UnMatched']=unMatched(output,CT_ED_Dic)
                #output['Customer']=output['Customer1']
                logger.info("Successfully matched Customer code in InActive AceesDB file")
                return output
            except Exception as e:
                print("\nError :->" + str(e) +"\n")
                logger.error("error occured while Updating Customer code from vista file %s" %e)
        except Exception as e:
            print("\nError :->" + str(e) +"\n")
            logger.error("Error occured while processing of Description,Pricing to 0 columns!!!! %s!" %e)

    ################## Extracting   desc##################

    def KMP_Mapping(KMP_CL,Metadata,Vista,KMP_CT):
        try:
            logger.info("Processing of KMP_OCH and Non_OCH data started!!!")
            temp4 = KMP_CL[['Jan','Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov','Dec']]
            temp4 = temp4.fillna(0)
            temp4 = temp4.astype(str).apply(lambda x : x+'-'+x.name)
            temp4 = temp4.values.tolist()
            temp5 =[]
            try:
                logger.info("Processing of Alt Agrrement and Date1 columns started!!!")
                for i in temp4:
                    temp5.append(','.join(map(str,[incom for incom in i if '0' not in incom])))
    
                KMP_CL['Des'] = temp5
                KMP_CL = KMP_CL.reset_index(drop=True)
                KMP_CL= KMP_CL[KMP_CL['Des'] != '']
                KMP_CL['Des'] = KMP_CL["Des"].str.split(',')
                KMP_CL=(KMP_CL
                .set_index(['ContractNumber','CustomerName','Address1','Address2'])['Des']
                .apply(pd.Series)
                .stack()
                .reset_index()
                .drop('level_4', axis=1)
                .rename(columns={0:'Description'}))
    
                KMP_CL1 = KMP_CL["Description"].str.split('-').str[1]
                KMP_CL2 = KMP_CL["Description"].str.split('-').str[0]
                var = []
                for i in KMP_CL1:
                    var.append(str(strptime(i,'%b').tm_mon) + "/01/2020")
                KMP_CL['Description'] = var
                var = []
                for i in KMP_CL2:
                    var.append(str(i))
                KMP_CL['Alt'] = var
                output = KMP_TraF(KMP_CL,KMP_CT)
                logger.info("Successfully Processed Alt Agreement and Date1 columns!!!")
                return output
            except Exception as e:
                print("\nError :->" + str(e) +"\n")
                logger.error("Error occurred while processing of Alt Agreement and Date1 column started!!! %s" %e)
        except Exception as e:
            print("\nError :->" + str(e) +"\n")
            logger.error("Error occurred while processing data for KMP OCH and Non_OCH!! %s" %e)


    ################################## KMP Active NON OCH Output File ##################################

    try:
        logger.info("Exporting of KMP Active NON OCH file Started!!")
        KMP_CL = pd.read_excel(KMP_data, sheet_name='Contract Lines')
        KMP_CL['Address2'] = KMP_CL['Address2'].str.replace("BUILDING CODE:| ", '')
        KMP_CL = KMP_CL[(KMP_CL['ContractStatus'] != 'IN')&(KMP_CL['CustomerName'] !='OTTAWA COMMUNITY HOUSING')&~(KMP_CL['CustomerName'].isin(ignore_list))]
        KMP_CT = pd.read_excel(KMP_data, sheet_name='Contracts')
        Vista =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
        output = KMP_Mapping(KMP_CL,Metadata,Vista,KMP_CT)
        print("\n--> Output KMP Active NON OCH file contains",len(output),"records")
        time.sleep(2)
        output.to_csv(KMP_NON_OCH,index=False)
        logger.info(" Successfully Exported KMP Active NON OCH file Started!!")
        print("\n--> KMP_NON_OCH Active File Generation Completed")
        time.sleep(2)
    except Exception as e:
        print("\nError :->" + str(e) +"\n")
        logger.error(" Error occurred while Exporting of KMP Active NON OCH file!! %s" %e)

        

    ################################## KMP Active OCH Output File ##################################

    try:
        logger.info("Exporting of KMP Active OCH file Started!!")
        KMP_CL = pd.read_excel(KMP_data, sheet_name='Contract Lines')
        KMP_CL['Address2'] = KMP_CL['Address2'].str.replace("BUILDING CODE:| ", '')
        KMP_CL = KMP_CL[(KMP_CL['ContractStatus'] != 'IN')&(KMP_CL['CustomerName'] =='OTTAWA COMMUNITY HOUSING')&(KMP_CL['ContractNumber']!='NON-CONTRACT')]
        KMP_CT = pd.read_excel(KMP_data, sheet_name='Contracts')
        Vista =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
        output = KMP_Mapping(KMP_CL,Metadata,Vista,KMP_CT)
        output['Customer'] = output['Alt Agreement'].apply(lambda x: '100051' if x == '0-100013' else '100052')
        print("\n--> Output KMP Active OCH file contains",len(output),"records")
        time.sleep(2)
        output.to_csv(KMP_OCH, index=False)
        logger.info("Succesfully Exported KMP Active OCH file Started!!")
        print("\n--> KMP_OCH Active File Generation Completed")
        time.sleep(2)
    except Exception as e:
        print("\nError :->" + str(e) +"\n")
        logger.error(" Error occured while Exporting of KMP Active OCH file!! %s" %e)


    ##################################  KMP InActive Output File ################################

    try:
        logger.info("Exporting of KMP InActive OCH file Started!!")
        KMP_CL = pd.read_excel(KMP_data, sheet_name='Contract Lines')
        KMP_CL['Address2'] = KMP_CL['Address2'].str.replace('BUILDING CODE:| ', '')
        KMP_CL1 = KMP_CL[(KMP_CL['CustomerName'].isin(ignore_list))]
        KMP_CL = KMP_CL[(KMP_CL['ContractStatus'] == 'IN') | (KMP_CL['ContractNumber']=='NON-CONTRACT')]
        KMP_CT = pd.read_excel(KMP_data, sheet_name='Contracts')
        VISTA =  pd.read_excel(VISTA_data,sheet_name='Sheet1')
        output=KMP_Mapping(KMP_CL,Metadata,Vista,KMP_CT)
        output1=KMP_Mapping(KMP_CL1,Metadata,Vista,KMP_CT)
        print("\n--> Output KMP Inactive file contains",len(output),"records")
        time.sleep(2)
        output.to_csv(KMP_InActive, index=False)
        output1.to_csv(ignorelist, index=False)
        print("\n--> KMP In_Active File Generation Completed\n")
        time.sleep(2)
    except Exception as e:
        print("\nError :->" + str(e) +"\n")
        logger.error("Error occured while Exporting of KMP InActive OCH file!! %s" %e)

except Exception as e:
    print("\nError :->" + str(e) +"\n")
    logger.error("Error occured while proccessing the Files.... %s" %e)



