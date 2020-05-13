include "json.h"

class JsonClient
{
    bool soap_status = true;
    bool conn_fail = true;
    soap *ctx = NULL;
    value request, response;
    string menpoint, mlogfile
}
pthread_mutex_t mutexfile = PTHREAD_MUTEX_INITIALIZER;
JsonClient::JsonClient(string serviceHost, string usrname, string pass)
{
  soap_status = true;
  conn_fail = false;
  menpoint = serviceHost;
  ctx = soap_new1(SOAP_C_UTFSTRING | SOAP_XML_INDENT);
  ctx->userid = usrname.data();
  ctx->passwd = pass.data();
  if (ctx == NULL)
  {
      soap_status = false;
  }
  ctx->max_keep_alive = 400;
  try
  {
      request = new_value(ctx);
  }
  catch (std::bad_alloc& e)
  {
      soap_status = false;
  }
  mlogfile = "./logs/JsonRequest.log";
  logstream.open(mlogfile.c_str(), std::fstream::out|std::fstream::app);
}

void JsonClient::setRequest(value& Req)
{
    request = req;
}

bool JsonClient::isValid()
{
    return soap_status;
}

bool JsonClient::isConnFailure()
{
    return conn_fail;
}

bool JsonClient::callService()
{
    signal(SIGPIPE, SIG_IGN);
    soap_ssl_init();
    if (soap_ssl_client_context(ctx, SOAP_SSL_NO_AUTHENTICATION, NULL, NULL, NULL, NULL, NULL))
    {
        soap_print_fault(ctx, stderr);
        soap_status = false;
        return false;
    }
    for (int i = 1; i <= 5; i++)
    {
        int retVl = json_call(ctx, menpoint, request, response);
        if (retVl == SOAP_OK)
        {
            retrun true;
        }
        
        pthread_mutex_lock (&mutexfile);
        if (ctx->error)
        {
            const char *json_out;
            json_out = (char*)ctx->userid;
            logstream << "Error while making call to" << menpoint << "with user" << json_out <<std::endl;
            soap_print_fault(ctx, stderr);
            logstream<<ctx->error<<std::endl;
            logstream<<"WARNING: trying in 2 minutes"<<std::endl;
        }
        
        if (response.has("error") && !response["error"].is_null())
        {
            logstream<<"WARNING: Server returned an error"<<std::endl;
            if (response["error"].has("code") && !response["error"]["code"].is_null())
            {
                logstream<<"Error Code: "<<response["error"]["code"];
                
            }
            if (response["error"].has("message") && !response["error"]["message"].is_null())
            {
                logstream<<"Error message: "<<response["error"]["message"];
                
            }
            else
            {
                logstream<<"Error message: "<<response["error"];
            }
        }
        else
        {
            logstream<<"Error message: "<<response <<std::endl<<"WARNING: trying in 2 minus" <<std::endl;
        }
        pthread_mutex_unlock(&mutexfile);
        sleep(120);
    }
    conn_fail = true;
    soap_status = false;
    return false;
}

JsonClient::~JsonClient()
{
    soap_destroy(ctx);  //delete object
    soap_end(ctx);      // delete data
    soap_free(ctx);    ///free context
}
