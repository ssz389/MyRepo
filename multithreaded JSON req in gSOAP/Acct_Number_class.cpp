/******************************************************************************

                              Online C++ Compiler.
               Code, Compile, Run and Debug C++ program online.
Write your code in this editor and press "Run" button to compile and execute it.

NOTE: This program is just a sample program to showcase the multithreading concept applied to handle millions of requests.
NOTE: May give compilation error due to incomplete code.
*******************************************************************************/

#include <iostream>
#include <pthread>
#include <extractClientJson>
//#include "json.h"

class Acct_number_cls
{
    protected:
    string ghost;
    string gpwd;
    string pwd;
    string mErrLogFile;
    string mConnFailure;
    string mDetokFailCnt;
    
    public:
    //constructor
    //destructor
    //few more member functions
    load_table(string);
};

struct ARG
{
    value request;
    int acc_cnt;
    Acct_numberObj = NULL;
}threadArgs;

void Acct_number_cls::load_table(string mbrActNum)
{
    long lrecCount;
    char sql[1000];
    static otl_connection *db = NULL;
    
    if (!db)
    {
        otl_connection::otl_initialize();
        db = new otl_connection();
        db->login();
    }
    try
    {
        sprintf(sql, "select count(1) from LOOKUP where Mbr_Account = %s ", mbrActNum);
        otl_stream otl1(1, (const char*) sql, *db);
        otl.scanf("%d", &lrecCount);
        
        char pwd[100 + 1];
        char usr[100 + 1];
        char hst[150 + 1];
        memset(pwd, 0x00, sizeof(pwd));
        memset(usr, 0x00, sizeof(pwd));
        memset(hst, 0x00, sizeof(pwd));
        sprintf(sql, "select trgt_host, trgt_usrId, trgt_pwd from CONFIG where Lookup = 'DECRYPT' ");
        
        otl_stream otl2(1, (const char*) sql, *db);
        if (otl2.eof())
        {
            otl2.scanf("%s %s %s", hst, usr, pwd);
            ghost = hst;
            gpwd = pwd;
            gusr = usr;
        }
        
        mbrActNumber = new (string mbrActNumber, int lrecCount);
        sprintf(sql, "select Mbr_Account, Acct_number from ACCT_LOOKUP where Mbr_Account = %s ", mbrActNum);
        otl_stream otl3(1, (const char*) sql, *db);
        char szMbrNum[11], szActNum[11];
        
        value request, response;
        int req_ctr = 0, upi_ctr = 0, nThread = 10, THRD_NUM = 0;
        bool isFirst = true;
        void *ret;
        pthread_t* tid = new pthread_t[nThread];
        while(otl3.eof())
        {
            otl3.scanf("%s %s", szMbrNum. szActNum);
            
            if(isFirst)
            {
                request = NULL;
                request["decrypt"]["bulk"]["id"] = upi_ctr + 1;
                request["decrypt"]["policyuser"] = "Pol1";
                request["decrypt"]["dataelementname"] = "COMM";
                isFirst = false;
                upi_ctr++;
            }
            
            string lAct_num(szActNum);
            string lMbr_num(szMbrNum);
            request["decrypt"]["bulk"]["data"][req_ctr]["content"] = lAct_num;
            //request["decrypt"]["bulk"]["data"][req_ctr]["id"] = req_ctr;
            request["decrypt"]["bulk"]["data"][req_ctr]["Member"] = lMbr_num;
            req_ctr++;
            
            if( req_ctr == 50000 )
            {
                threadArgs *arg = new threadArgs;
                
                arg->request = request;
                arg->acc_cnt = req_ctr - 1;
                arg->Acct_numberObj = this;
                if( pthread_create(&tid[THRD_NUM], NULL, &process_request, (void*)arg != 0 ))
                {
                    Exception c("pthread_create failed.");
                    throw c;
                }
                THRD_NUM++;
                if(THRD_NUM == nThread)   // wait untill 10 requests are completed
                {
                    for (int i = 0; i < THRD_NUM; i++)
                    {
                        if(pthread_join(tid[i], &ret) != 0)
                        {
                            Exception c("pthread_join failed.");
                            throw c;
                        }
                    }
                    THRD_NUM = 0;
                }
                isFirst = true;
                req_ctr = 0;
            }
        }
        
        if( req_ctr > 0 )
        {
            threadArgs *arg = new threadArgs;
                
            arg->request = request;
            arg->acc_cnt = req_ctr - 1;
            arg->Acct_numberObj = this;
            if( pthread_create(&tid[THRD_NUM], NULL, &process_request, (void*)arg != 0 ))
            {
                Exception c("pthread_create failed.");
                throw c;
            }
        }
        
        if(!isFirst)   // wait untill 10 requests are completed
        {
            for (int i = 0; i < THRD_NUM; i++)
            {
                if(pthread_join(tid[i], &ret) != 0)
                {
                    Exception c("pthread_join failed.");
                    throw c;
                }
            }
            isFirst = true;
            req_ctr = 0;
        }
        
        delete[] tid;
        return;
    }
    catch(Exception e)
    {
        throw(e);
    }
    catch(otl_Exception e)
    {
        //raise otl_Exception
        throw(otl_Exception_var);
    }
    catch(std::bad_alloc& e)
    {
        throw(e);
    }
}  //end load_table

void *process_request(void *arguments)
{
    sruct threadArgs *Args = (struct threadArgs *) arguments;
    value request = Args->request;
    int req_ctr = Args->acc_cnt;
    bool isSoap_OK = false;
    value response;
    
    extractJsonClient *jsonClientObj = NULL;
    jsonClientObj = new JsonClient(Args->Acct_numberObj->ghost, Args->Acct_numberObj->gusr, Args->Acct_numberObj->gpwd );
    jsonClientObj->setRequest(request);
    if(jsonClientObj != NULL && jsonClientObj->isValid())
    {
        isSoap_OK = jsonClientObj->callService();
        if (isSoap_OK)
        {
            response = jsonClientObj->getResponse();
        }
    }
    
    int res_ctr = 0;
    if(isSoap_OK && jsonClientObj->isValid())
    {
        if(response["decrypt"]["bulk"].has("data") && !response["decrypt"]["bulk"]["data"].is_null())
        {
            while( res_ctr <= req_ctr )
            {
                if(response["decrypt"]["bulk"]["data"][res_ctr].has("content") && !response["decrypt"]["bulk"]["data"][res_ctr]["content"].is_null())
                {
                    string lMbrActNum = "";
                    string detokActNum = response["decrypt"]["bulk"]["data"][res_ctr]["content"];
                    string mbrActNum = response["decrypt"]["bulk"]["data"][res_ctr]["Member"];
                    lMbrActNum = mbrActNum;
                    lMbrActNum += detokActNum;
                    pthread_mutex_lock(&mutexhash);
                    Args->Acct_numberObj->mbrActNumber->insert(lMbrActNum);
                    pthread_mutex_unlock(&mutexhash);
                }
                res_ctr++;
            }
        }
    }
    else
    {
        if(jsonClientObj != NULL && jsonClientObj->isConnectionFailure())
        {
            pthread_mutex_lock(&mutexhash);
            Args->Acct_numberObj->mErrLogFile = jsonClientObj->getLogFile();
            Args->Acct_numberObj->mConnFailure = true;
            Args->Acct_numberObj->mDetokFailCnt += req_ctr + 1;
            pthread_mutex_unlock(&mutexhash);
        }
        
        if(request["decrypt"]["bulk"]..has("data") && !request["decrypt"]["bulk"]["data"].is_null())
        {
            while( res_ctr <= req_ctr )
            {
                if(request["decrypt"]["bulk"]["data"][res_ctr].has("content") && !request["decrypt"]["bulk"]["data"][res_ctr]["content"].is_null())
                {
                    string lMbrActNum = "";
                    string detokActNum = request["decrypt"]["bulk"]["data"][res_ctr]["content"];
                    string mbrActNum = request["decrypt"]["bulk"]["data"][res_ctr]["Member"];
                    lMbrActNum = mbrActNum;
                    lMbrActNum += detokActNum;
                    pthread_mutex_lock(&mutexhash);
                    Args->Acct_numberObj->mbrActNumber->insert(lMbrActNum);
                    pthread_mutex_unlock(&mutexhash);
                }
                res_ctr++;
            }
        }
    }
    
    if(jsonClientObj)
    {
        delete jsonClientObj;
        jsonClientObj = NULL;
    }
    
    return NULL;
}

