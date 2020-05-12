/******************************************************************************

                              Online C++ Compiler.
               Code, Compile, Run and Debug C++ program online.
Write your code in this editor and press "Run" button to compile and execute it.

*******************************************************************************/

#include <iostream>
#include <pthread>

int AVRCreartor::initiate()
{
    try
    {
        char szTrans[MAX_REC_SIZE + 1];
        char szRaw[MAX_REC_SIZE + 1];
        memset( szTrans, 0x00, MAX_REC_SIZE + 1);
        memset( szRaw, 0x00, MAX_REC_SIZE + 1);
        
        int ilen = 0, prevlen = 0;
        //load CSVorFixedlength file
        //load segment layout of the file
        //load fields of each segments
        
        //create a recordobject
        //read each record of the file
        while((ilen = recordobject->readrecord(szTrans, szRaw)))
        {
            if (prevlen > ilen)
            {
                memset( szTrans + ilen + 1, 0x00, prevlen - ilen);
                memset( szRaw + ilen + 1, 0x00, prevlen - ilen);
            }
            prevlen = ilen;
            
            if(firstRecord)
            {
                loadAVRJSONSchema();
                firstRecord = false;
            }
            
            if(!processAVRRecord())
            {
                return -255;
            }
            
            writeToOutputFile();
        }
    }
    catch (avro::Exception& e)
    {
        std::cout<<e.what()<<std::endl;
        return -255;
    }
    catch(...)
    {
        std::cout<<"Unkown Exception"<<std::endl;
        return -255;
    }
    return 0;
}

bool AVRCreartor::loadAVRJSONSchema()
{
    string schema_Path = "/bin/";
    //read the appropriate JSON schema based on the FileType and 
    //a trigger field specific to each record being populated in the first record of the file
    //trigger field(pf field object pointer) id will be mentioned in the segment layout of the record/file
    for (each avrorecord type)
    {
        //if (check if trade trigger field is present)
        //{
            //create field pointer pf;
    
            if (pf != NULL && !pf->fValue.isnull())
            {
                JSON_File = schema_Path + "/TradeRecordAVRO.json";
                avro::Validschema avroschema = loadSchema(JSON_File)
                //different json schemas based on record type: debtRecovery , collection, legal, bankruptcy, consolDebt 
                pTradeDataWriter = new avro::DataFileWriter<extractTrade::TradeRecord>(sOutFile, avroschema)
                //set a recordtype variable
                recordtype = eTRADE;
                return true;
            }
        //}
        //else if(check if other trigger field for other segments is present in the file)
        //{
            //...
        //}
    }
    return false;
}

bool AVRCreartor::processAVRRecord()
{
    switch(recordtype)
    {
        case eTRADE:
            buildTradeRecord();
            break;
        case eDEBTREC:
            buildDebtRecRecord();
            break;
        //likewise for all other record types
        default:
            return false;
            break;
    }
}

bool AVRCreartor::buildTradeRecord()
{
    //read the recordobject to read the segment and field's fValue
    //build avro blocks one by one
    std::vector<extractTrade::PIIRecord> vPIIRec;
    std::vector<extractTrade::DOBRecord> vDOBRec;
    std::vector<extractTrade::NameRecord> vNmRec;
    //... likewise create vector for ach array type of the block from loaded json schema
    for(each segment in the loaded segment layout of the input file)
    {
        switch(segType)
        {
            case eTRADE:
                buildTradeBlock(seginfo, pTradeRec.tradeDetail);
                break;
            case eNAME:
            {
                extractTrade::NameRecord pNameRec;
                buildNameBlock < segmentDesc, std::vector<extractTrade::NameRecord>, extractTrade::NameRecord > (segInfo, vNmRec, pNameRec);
                break;
            }
            case eDOB1:
            case eDOB2:
            {
                extractTrade::DOBRecord pDOBRec;
                buildDOBBlock < segmentDesc, std::vector<extractTrade::DOBRecord>, extractTrade::DOBRecord > (segInfo, vNmRec, pNameRec);
                break;
            }
            //likewise for address, phone etc.
            case ePII:
            {
                extractTrade::PIIRecord pPIIRec;
                buildPIIBlock < segmentDesc, std::vector<extractTrade::PIIRecord>, std::vector<extractTrade::NameRecord>, std::vector<extractTrade::DOBRecord>, extractTrade::DOBRecord > (segInfo, vNmRec, pNameRec);
                break;
            }
        }
    }
    return true;
}

void AVRCreartor::buildTradeBlock(segmentDesc &curseg, extractTrade::TradeDetailRecord &pTradeBlock)
{
    //create a field pointer const baseField *pf = NULL;
    int segFieldcnt = curseg.fieldList.size();
    
    if (!validseg(curseg, 0))
    {
        return;
    }
    
    for (int i = 0; i < segFieldcnt; i++)
    {
        pf = pRec->findfield(curseg.fieldList[i].fieldId);
        if (pf)
        {
            string fieldValue = pf->fValue;
            setStructureVariableTrade<int, extractTrade::TradeDetailRecord, const char* > ((int)curseg.fieldList[i].fieldId, &pTradeBlock, fieldValue.data());
        }
        else if (curseg.fieldList[i].subfiledPos != 0 )
        {
            std::string svalue = "0";
            if (ISTRUE(curseg.fieldList[i].parentId, curseg.fieldList[i].fieldId))
            {
                svalue = "1"
            }
            setStructureVariableTrade<int, extractTrade::TradeDetailRecord, const char* > ((int)curseg.fieldList[i].fieldId, &pTradeBlock, sValue.data());
        }
    }
    return;
}
