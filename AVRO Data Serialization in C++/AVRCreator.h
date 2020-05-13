#include <vector>
#include "avro/Encoder.hh"
#include "avro/Decode.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "TradeRecordAvro.hh"
#include "LegalRecordAvro.hh"
#include "DebtRecoveryRecordAvro.hh"
#include "BankruptcyRecordAvro.hh"
#include "CollectionRecordAvro.hh"
#include "ConsolDebtRecordAvro.hh"
#include "NameRecord.h"
#include "DateOfBirthRecord.h"
#include "TradeDetailRecord.h"
#include "PiiRecord.h"
#include "SinRecord.h"
#include "AddressRecord.h"
#include "PhoneRecord.h"

typedef struct
{
    std::string avroFieldName;
    std::string avroFieldType;
    FieldEnum FieldID;
    int subfieldpos;
    FieldEnum ParentFieldID;
} fielddesc;

class AVRCreator
{
    protected:
    std::string JSON_File;
    extractTrade::TradeRecord pTradeRec;
    baseRecord *pRec;
    avro::DataFileWriter<extractTrade::TradeRecord>* pTradeDataWriter;
    std::string sOutfile;
    public:
    //constructor and Destructor
    virtual int initiate();
    bool loadAVRJSONSchema();
    bool buildTradeRecord();
    bool processAVRRecord();
    void writeToOutputFile();
    void buildTradeBlock(segmentDesc &curseg, extractTrade::TradeDetailRecord &pTradeBlock);
    tempplate <typename T1, typename T2, typename T3>
    void buildDOBBlock(T1 &curseg, T2 &vDOBRec, T3 pDOBRec);
    tempplate <typename T1, typename T2, typename T3>
    void buildNameBlock(T1 &curseg, T2 &vNameRec, T3 pNameRec);
    tempplate <typename T1, typename T2, typename T3, typename T4, typename T5>
    void buildPIIBlock(T1 &curseg, T2 &vPIIRec, T3 &vNameRec, T4 &vDOBRec, T4 pPIIRec);
};
