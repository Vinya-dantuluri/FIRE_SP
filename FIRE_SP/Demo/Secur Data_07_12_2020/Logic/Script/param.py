basepath = r"D:\FSP_GITHUB\FIRE_SP\Demo\Secur Data_07_12_2020\\"

ADB_data = basepath+'Source\Secur Export - 2020 12 03.xlsx'
KMP_data = basepath+'Source\KMP Data.xlsx'

Metadata = basepath+'Logic\Metadata\Output.xlsx'
VISTA_data = basepath+'Logic\Metadata\mapping.xlsx'
ignore_list = basepath+'Source\ignorelist.xlsx'
FnR = basepath+'Logic\Metadata\FnR.csv'

KMPLog = basepath+'Logs\KMP\info.log'
AcessLog = basepath+'Logs\Accessdb\info.log'

KMP_NON_OCH = basepath+'Target\KMP\A_KMP_NON_OCH.csv'
KMP_OCH = basepath+'Target\KMP\A_KMP_OCH.csv'
KMP_InActive = basepath+'Target\KMP\IA_KMP.csv'
ignorelist_KMP = basepath+'Target\KMP\K_IA_ignore_list.csv'
ignorelist_Access_A = basepath+'Target\Accessdb\A_IA_ignore_list.csv'
ignorelist_Access_I = basepath+'Target\Accessdb\I_IA_ignore_list.csv'
ActiveAccess = basepath+'Target\Accessdb\A_AccessDB.csv'
InactiveAccess = basepath+'Target\Accessdb\IA_AccessDB.csv'
monitoring_Active=basepath+'Target\Accessdb\AM_AccessDB.csv'
monitoring_InActive=basepath+'Target\Accessdb\IAM_AccessDB.csv'

