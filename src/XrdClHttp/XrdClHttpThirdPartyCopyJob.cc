#include "XrdClHttpThirdPartyCopyJob.hh"
#include "XrdClHttpOps.hh"

#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClUtils.hh"

#include <atomic>
#include <fstream>
#include <mutex>
#include <condition_variable>

using namespace XrdClHttp;

class ThirdPartyCopyResponseHandler : public XrdCl::ResponseHandler
{
private:
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic_bool ready{false};

public:
    virtual ~ThirdPartyCopyResponseHandler() = default;

    virtual void HandleResponse(XrdCl::XRootDStatus *status,
                                XrdCl::AnyObject    *response )
    {
        {
            std::lock_guard lock(mutex);
            ready.store(true);
        }
        cv.notify_all();
    }

    void wait()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this]{ return this->ready.load(); });
        ready.store(false);
    }
};

ThirdPartyCopy::ThirdPartyCopy(uint32_t      jobId,
                                XrdCl::PropertyList *jobProperties,
                                XrdCl::PropertyList *jobResults,
                                std::shared_ptr<XrdClHttp::HandlerQueue> queue) :
    CopyJob(jobId, jobProperties, jobResults),
    m_queue(queue)
{
    const auto &p = jobProperties;
    for (const auto &item : *p)
        std::cout << item.first << ":" << item.second << std::endl;
        
    XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();
    log->Debug( XrdCl::UtilityMsg, "Creating a HTTP third party copy job, from %s to %s",
                GetSource().GetObfuscatedURL().c_str(), GetTarget().GetObfuscatedURL().c_str() );
}

XrdCl::XRootDStatus ThirdPartyCopy::Run(XrdCl::CopyProgressHandler *progress)
{
    ThirdPartyCopyResponseHandler rh;

    XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();
    log->Debug(kLogXrdClHttp, "XrdClHttp::ThirdPartyCopy Copy Op src %s dst %s", GetSource().GetURL().c_str(), GetTarget().GetURL().c_str());

    std::size_t size = 0;

    try
    {
        std::shared_ptr<CurlStatOp> op_stat(new CurlStatOp(&rh, GetSource().GetURL(), {10,0}, log, true, nullptr, nullptr));
        m_queue->Produce(op_stat);

        rh.wait();

        if (!op_stat->HasFailed())
            size = op_stat->GetStatInfo().first;
        else
            log->Warning(kLogXrdClHttp, "Failed to get source file size");
    }
    catch (...) {
        log->Warning(kLogXrdClHttp, "Failed to add stat op to queue");
    }

    bool is_pull = pProperties->Get<std::string>("thirdPartyMode") != "push";
    int streams = 1;
    time_t tpc_timeout = 0;

    pProperties->Get("tpcTimeout", tpc_timeout);

#if 0 // disabled until XrdHttpTPC bug is fixed
    if (auto env = XrdCl::DefaultEnv::GetEnv(); env)
        env->GetInt("SubStreamsPerChannel", streams);
#endif

    CurlCopyOp::Headers headers;
    CurlCopyOp::Headers src_hdrs;
    CurlCopyOp::Headers dst_hdrs;

    if (const auto token_file = pProperties->Get<std::string>("thirdPartyTokenFile"); !token_file.empty())
    {
        if (std::ifstream file(token_file); file.is_open())
        {
            const auto read_token = [&file] (CurlCopyOp::Headers &headers)
            {
                if(std::string line; std::getline(file, line))
                    headers.emplace_back("Authorization", std::string("Bearer ") + line);
            };

            read_token(src_hdrs);
            read_token(dst_hdrs);
        }
        else
        {
            log->Warning(kLogXrdClHttp, "Failed to open token file");
            return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errAuthFailed);
        }
    }

    headers.emplace_back("X-Number-Of-Streams", std::to_string(streams));
    headers.emplace_back("Overwrite", pProperties->Get<std::string>("force") == "1" ? "T" : "F");

    std::shared_ptr<CurlCopyOp> op_copy(new CurlCopyOp(&rh, GetSource().GetURL(), src_hdrs, GetTarget().GetURL(), dst_hdrs, headers, is_pull, {tpc_timeout, 0}, log, nullptr));
    op_copy->SetCallback([this, progress, size] (auto bytemark)
        {
            progress->JobProgress(this->pJobId, bytemark, size);
        });

    try
    {
        m_queue->Produce(op_copy);
    } 
    catch (...) {
        log->Warning(kLogXrdClHttp, "Failed to add copy op to queue");
        return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errOSError);
    }

    rh.wait();

    if (!op_copy->IsSentSucessfully())
        return {XrdCl::stError, XrdCl::errPipelineFailed, 0, op_copy->GetSendingFailureMessage()};

    if (size > 0)
        progress->JobProgress(pJobId, size, size);

    return XrdCl::XRootDStatus();
}
