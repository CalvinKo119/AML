import pandas as pd
import mysql.connector
from datetime import datetime
from datetime import date
from dateutil.relativedelta import *
##connect to the database
mydb = mysql.connector.connect(     #connect to database
  host="121.36.52.202",
  user="hase_test",
  password="hase_test@Abc",
  database="hackathon"
)
mycur=mydb.cursor()              #call cursor module under connect
mycur.execute("SELECT AccountNumber, TransactionTime, TranType, TranAmt, RelAccountNumber FROM account_posting") #mysql query
result_account=mycur.fetchall()   #Extract and store all records in data table in list
mycur.execute("SELECT CurrentAccountNumber, SavingAccountNumber, DateOfBirth FROM customer_data")  #mysql query
result_cus=mycur.fetchall()     #Extract and store all records in data table in list
mydb.close()                 #close the connection with database
##COnvert the data table extracted into dataframe
acc_data=pd.DataFrame(result_account)   
cus_data=pd.DataFrame(result_cus)
acc_data.rename(columns={0:'AccountNo',1:'TranTime',2:'TranType',3:'TranAmt',4:'RelAccountNo'},inplace=True)    #rename the useful dataframe column
cus_data.rename(columns={0:'CurrentAccountNo',1:'SavingAccountNo',2:"DateOfBirth"},inplace=True)
##Calculate the ages for all records
cus_data['DateOfBirth']=cus_data['DateOfBirth'].astype(str)  #convert the values in column'DateOfBirth' from object to string
cus_data['DateOfBirth']=pd.to_datetime(cus_data['DateOfBirth'], format='%Y-%m-%d') #convert the value from string to datetime format
Age=[]   
days_in_year=365 
for i in range(len(cus_data.index)):
    Age.append(int((date.today() - cus_data['DateOfBirth'][i].date()).days / days_in_year)) #store the ages of each records into the list 'Age'
cus_data['Age']=Age  #Substitute the list 'Age' into teh dataframe cus_data as a column 'Age'
##Merge the 2 dataframes cus_data & acc_data to give single dataframe cus_acc
acc_data['AccountNo']=acc_data['AccountNo'].astype(str)     #convert the datatype of account number related columns to string
cus_data['CurrentAccountNo']=cus_data['CurrentAccountNo'].astype(str)
cus_data['SavingAccountNo']=cus_data['SavingAccountNo'].astype(str)
cus_sav=pd.merge(cus_data,acc_data,left_on='SavingAccountNo',right_on='AccountNo',how='inner')   #merge cus_data and acc_data on Saving account no.
cus_cur=pd.merge(cus_data,acc_data,left_on='CurrentAccountNo',right_on='AccountNo',how='inner')  #merge cus_data and acc_data on current account no.
cus_acc=pd.concat([cus_cur,cus_sav]) #concat the above 2 table as well 
##Filter away the account transaction others than transfer in(0004) and transfer out(0005)
cus_acc['TranType']=cus_acc['TranType'].astype(str)
cus_acc_T=cus_acc[(cus_acc['TranType']=='0004')|(cus_acc['TranType']=='0005')]
cus_acc_T.reset_index(drop=True,inplace=True)
##Group the transaction records which have the same AccountNo and RelAccountNo together 
cus_acc_T.groupby(['AccountNo','RelAccountNo'])
cus_acc_T.reset_index(drop=True,inplace=True)
##Filter away the records which have less than 25 tranactions in 3 years
tmp_freq=cus_acc_T.groupby(['AccountNo','RelAccountNo']).size().reset_index(name='count') #the frequency of occurance of each group in 3 years is stored in dataframe tmp_freq in column 'count'
print(tmp_freq)
tmp_freq_below25=tmp_freq[tmp_freq['count']<25] #filtering
tmp_freq_above25=tmp_freq[tmp_freq['count']>=25] #filtering the records with total freq >=25 in 3 years
tmp_freq_below25.reset_index(drop=True,inplace=True)
tmp_freq_above25.reset_index(drop=True,inplace=True)
cus_acc_h_freq=pd.merge(cus_acc_T,tmp_freq_above25,on=['AccountNo','RelAccountNo'], how='inner') #filter the transaction records using merge function 
##Calculae the age of the target accounts holders of each transaction
tmp_RelAcc=''
Age_Rel=[]
i=0
while (i<=len(cus_acc_h_freq)-1):  #The following loops check the RelAccount of each transactions records and then the age of that RelAccount holder from cus_data dataframe 
    if (tmp_RelAcc!=cus_acc_h_freq['RelAccountNo'][i]):
        tmp_RelAcc=cus_acc_h_freq['RelAccountNo'][i]
        for j in range(len(cus_data.index)):
            if ((cus_data['SavingAccountNo'][j]==tmp_RelAcc) or (cus_data['CurrentAccountNo'][j]==tmp_RelAcc)):
                Age=cus_data['Age'][j]    #Find the corresponding RelAccount holder age from cus_data dataframe
                Age_Rel.append(Age)       #Store them as list   
                break
    else:
        Age_Rel.append(Age)           
    i=i+1
cus_acc_h_freq['Age_Rel']=Age_Rel               
##Filter away the records which are transaction between two customers below 70 years old
cus_acc_h_freq=cus_acc_h_freq[~((cus_acc_h_freq['Age']<70)&(cus_acc_h_freq['Age_Rel']<70))]
cus_acc_h_freq.reset_index(inplace=True,drop=True)
##Find the highest frequency of transactions between 2 accounts in 3 months and record the corresponding period
cus_acc_h_freq['TranTime']=cus_acc_h_freq['TranTime'].astype(str)
cus_acc_h_freq['TranTime']=pd.to_datetime(cus_acc_h_freq['TranTime'], format='%Y%m%d%H%M%S')
AML_suspect_tran=cus_acc_h_freq.groupby(['AccountNo','RelAccountNo']).size().reset_index(name='No. of transactions in 3 years') #the frequency of occurance of each group in 3 years is stored in dataframe AML_suspect_tran
print(AML_suspect_tran)
start=[] # store the suspious starting date of money laundering
end=[] # store the suspious ending date of money laudering
No_trans_3_months=[] #store the freq of transactions during period of money laundering                      
for i in range(len(AML_suspect_tran.index)):          #initialize the 3 lists
    start.append(datetime(1900,1,1,00,00,00))
    end.append(datetime(1900,1,1,00,00,00))
    No_trans_3_months.append(-1)
increment=relativedelta(days=1)        #parameter used in the iteration for searching the suspious period
period=relativedelta(months=3)         #duration of the period
for i in range(len(AML_suspect_tran.index)):
    initial=datetime(2017,1,1,00,00,00)
    final=initial+period
    while(final<=(datetime(2017,1,1,00,00,00)+relativedelta(years=3))): #iteration ends at final=2019/12/31
        Number_tran=len(cus_acc_h_freq[(cus_acc_h_freq['AccountNo']==AML_suspect_tran['AccountNo'][i])&
                                        (cus_acc_h_freq['RelAccountNo']==AML_suspect_tran['RelAccountNo'][i])&
                                        (cus_acc_h_freq['TranTime']<=final)&(cus_acc_h_freq['TranTime']>initial)])
        if(Number_tran>No_trans_3_months[i]):    #store the info. of that period once its freq is find to be higher than the previous one
            No_trans_3_months[i]=Number_tran
            start[i]=initial
            end[i]=final
        initial=initial+increment         #iteration
        final=final+increment
AML_suspect_tran['Frequency of transaction in 3 months']=No_trans_3_months  #store the info. of the suspious period in the dataframe
AML_suspect_tran['Start from']=start
AML_suspect_tran['end at']=end
##Filter away the records which has less than 25 transactions in 3 months period
AML_suspect_tran=AML_suspect_tran[AML_suspect_tran['Frequency of transaction in 3 months']>=25]
AML_suspect_tran.reset_index(inplace=True,drop=True)
##delete the 'No. of transactions in 3 years' column in the dataframe and output the dataframe as .csv file
AML_suspect_tran.drop('No. of transactions in 3 years',axis='columns', inplace=True)
AML_suspect_tran.to_csv(r'AML_suspect_tran_elderly.csv',index=False, header=True)
