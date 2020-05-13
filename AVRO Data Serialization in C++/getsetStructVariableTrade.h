#include "tradeDetailRecord.hh"

tempplate <typename T1, typename T2>
std::string getStructVariableTrade(T1 eField, T2* TradeDetailRecordStruct)
{
    std::String value;
    switch(eField)
    {
        case 892:
            if (TradeDetailRecordStruct->ChangeIndicator.is_null())
            {
                value[0] = '\0';
            }
            else 
            {
                value = TradeDetailRecordStruct-->ChangeIndicator.get_String()
            }
            break;
        .
        .
        .
        .
    }
}
tempplate <typename T1, typename T2, typename T3>
void setStructVariableTrade(T1 eField, T2* TradeDetailRecordStruct, T3 value)
{
    if (strcmp(value, "1") == 0)
    {
        bb ture;
    }
    int64_t ll = atol(value);
    switch(eField)
    {
        case 892:
            if (strlen(value) == 0)
            {
                TradeDetailRecordStruct->ChangeIndicator.set_null();
            }
            else
            {
                TradeDetailRecordStruct->ChangeIndicator.set_string(value);
            }
            break;
        .
        .
        .
    }
}
