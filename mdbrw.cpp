#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <set>
#include <vector>
#include <time.h>
#include <string>
#include <sys/time.h>
#include "lmdb.h"
using namespace std;

long int g_time;
tm nowTime;
char current[1024];
#define DebugInfo(fmt,...)\
        g_time = time(NULL); \
        localtime_r(&g_time, &nowTime);\
        sprintf(current, "%02d/%02d %02d:%02d:%02d", nowTime.tm_mon+1, nowTime.tm_mday, \
        nowTime.tm_hour, nowTime.tm_min, nowTime.tm_sec);\
        printf(" \033[33m%s %s %d -- " fmt "\033[0m\n", current,  __func__, __LINE__, __VA_ARGS__);\

#define DebugErr(fmt,...)\
        g_time = time(NULL); \
        localtime_r(&g_time, &nowTime);\
        sprintf(current, "%02d/%02d %02d:%02d:%02d", nowTime.tm_mon+1, nowTime.tm_mday, \
        nowTime.tm_hour, nowTime.tm_min, nowTime.tm_sec);\
        printf(" \033[41m%s %s %d -- " fmt "\033[0m\n", current, __func__, __LINE__, __VA_ARGS__);\



class MDB
{

public:


    char m_cFilePath[300];
    int m_nFileSize;
    int m_nType;
    MDB_val key;
    MDB_val data;
    struct MdbEnv
    {
    	MDB_env *env;
    	MDB_dbi dbi;
    	MDB_txn *txn;
    	MDB_cursor *cursor;
    	MdbEnv()
    	: env(NULL)
        ,dbi(0)
    	, txn(NULL)
    	, cursor(NULL)
    	{ }
    }m_MdbEnv; 

    MDB();
    bool InitMdb();       
    bool FreeMdbEnv();      
    bool MdbGet(char* cKey, char* cVal);
    bool MdbCursorGet(char* cVal);
    bool MdbWrite(char* cKey, char* cVal);
    void SetKey(char* key);
    void SetVal(char* val);
    void GetVal(char* val);

    ~MDB()
    {
            FreeMdbEnv();
    }
};

void MDB::SetKey(char* key)
{
        this->key.mv_data = key;
        this->key.mv_size = strlen(key) + 1;
}

void MDB::SetVal(char* val)
{
         this->data.mv_data=val;
         this->data.mv_size = strlen(val) + 1;
}

void MDB::GetVal(char* val)
{   
        strcpy(val, (char*)data.mv_data);
        DebugInfo("val:%s", val);
}

MDB::MDB()
{
        m_nFileSize = 200*1024*1024;
        m_nType = 0;
        strcpy(m_cFilePath,"./");
        InitMdb();
}


bool MDB::InitMdb()
{
        if(mdb_env_create(&m_MdbEnv.env) == 0){
                DebugInfo("%s", "mdb_env_create success");
        }else{
                DebugErr("%s", "mdb_env_create fail!");
                return false;
        }
        
        if(mdb_env_set_mapsize(m_MdbEnv.env, 200*1024*1024) == 0){
                DebugInfo("%s", "mdb_env_set_mapsize success");
        }else{
                DebugErr("%s", "mdb_env_set_mapsize fail!");
                return false;
        }

        if(mdb_env_open(m_MdbEnv.env, m_cFilePath, 0, 0644)==0){
                   DebugInfo("%s", "mdb_env_open success");
        }else{
                DebugErr("%s", "mdb_env_open fail!");
                return false;
        }

        return true;
}

 bool MDB::MdbCursorGet(char* cVal)
 {
        m_nType = 1;
         if(m_MdbEnv.dbi==0){
                if(mdb_txn_begin(m_MdbEnv.env, NULL,MDB_RDONLY, &m_MdbEnv.txn) || mdb_cursor_open(m_MdbEnv.txn, m_MdbEnv.dbi, &m_MdbEnv.cursor)){
                        DebugInfo("%s", "mdb_cursor_open success");
                } else{
                        DebugErr("%s", "mdb_cursor_open fail!");
                        return false;
                }
        }else{
                int nRet = mdb_cursor_get(m_MdbEnv.cursor, &key, &data, MDB_NEXT);
                if(nRet != 0){
                            DebugErr("mdb_cursor_get fail %d", nRet);
                            return false;
                }
                GetVal(cVal);
        }

        return true;
 }

 bool MDB::MdbGet(char* cKey, char* cVal)
 {
         SetKey(cKey);
         int nRet = 0;
         if(m_MdbEnv.dbi==0){
                    nRet = mdb_txn_begin(m_MdbEnv.env, NULL,MDB_RDONLY, &m_MdbEnv.txn) || mdb_open(m_MdbEnv.txn, NULL, 0, &m_MdbEnv.dbi);
                    if( nRet == 0){
                            DebugInfo("%s", "mdb_open success");
                    } else{
                            DebugErr("mdb_open fail %d", nRet);
                            return false;
                    }
        }

        DebugInfo("key:%s", (char*)key.mv_data);
        nRet =mdb_get(m_MdbEnv.txn, m_MdbEnv.dbi,  &key, &data);
        if(nRet != 0){
                    DebugErr("mdb_get fail %d", nRet);
                    return false;
        }
        GetVal(cVal);
        return true;
 }  

bool  MDB::FreeMdbEnv()
{
        DebugInfo("%d", m_nType);
       

        if(m_nType==1){
                  if(m_MdbEnv.cursor == NULL){
                          return true;
                }else{
                          mdb_cursor_close(m_MdbEnv.cursor);  //关闭游标
                }
        }

         if(m_MdbEnv.txn == NULL){
                        return true;
         }

        if(m_nType==2){
                  mdb_txn_commit(m_MdbEnv.txn);   
        }else{
		 mdb_txn_abort(m_MdbEnv.txn);
	}

        mdb_close(m_MdbEnv.env, m_MdbEnv.dbi);
        mdb_env_close(m_MdbEnv.env);
        m_MdbEnv.env = NULL;
        return true;
}

bool MDB::MdbWrite(char* cKey, char* cVal)
{
         m_nType = 2;
         SetKey(cKey);
         SetVal(cVal);
          int nRet = 0;
         if(m_MdbEnv.dbi==0){
                    mdb_txn_begin(m_MdbEnv.env, NULL,0, &m_MdbEnv.txn) ||mdb_open(m_MdbEnv.txn, NULL, 0, &m_MdbEnv.dbi);
                    if(nRet == 0){
                            DebugInfo("%s", "mdb_open success");
                    } else{
                            DebugErr("mdb_open fail %d", nRet);
                            return false;
                    }
        }
        DebugInfo("key:%s, val:%s", (char*)key.mv_data, (char*)data.mv_data);
        nRet =mdb_put(m_MdbEnv.txn, m_MdbEnv.dbi, &key, &data, MDB_NODUPDATA);
        if(nRet != 0){
                    DebugErr("mdb_put fail %d", nRet);
                    return false;
        }else{
                    DebugInfo("%s", "mdb_put success");
        }
        return true;
}

int main(int argc , char *argv[])
{
              //MDB TestWrite;
              MDB TestRead;
              char cKey[20] = "00001";
              char cVal[20] = "";
             //TestWrite.MdbWrite(cKey, cVal);
              TestRead.MdbGet(cKey, cVal); 
	       return 0; 
}
