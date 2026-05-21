#include "XrdClHttpThirdPartyCopyJob.hh"
#include "XrdClHttpOps.hh"

#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClUtils.hh"

#include <thread>

using namespace XrdClHttp;

ThirdPartyCopy::ThirdPartyCopy(uint32_t      jobId,
                                XrdCl::PropertyList *jobProperties,
                                XrdCl::PropertyList *jobResults,
                                std::shared_ptr<XrdClHttp::HandlerQueue> queue,
                                CreateConnCalloutType callout) :
    CopyJob(jobId, jobProperties, jobResults),
    m_queue(queue)
{
    XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();
    log->Debug( XrdCl::UtilityMsg, "Creating a HTTP third party copy job, from %s to %s",
                GetSource().GetObfuscatedURL().c_str(), GetTarget().GetObfuscatedURL().c_str() );
}

XrdCl::XRootDStatus ThirdPartyCopy::Run(XrdCl::CopyProgressHandler *progress)
{
    XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();

    log->Debug(kLogXrdClHttp, "XrdClHttp::ThirdPartyCopy Copy Op src %s dst %s", GetSource().GetURL().c_str(), GetTarget().GetURL().c_str());

    std::shared_ptr<CurlStatOp> op_stat(new CurlStatOp(nullptr, GetSource().GetURL(), {10,0}, log, true, nullptr, nullptr));

    try {
        m_queue->Produce(op_stat);
    } catch (...) {
        log->Warning(kLogXrdClHttp, "Failed to add stat op to queue");
        return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errOSError);
    }

    while (!op_stat->IsDone())
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (op_stat->HasFailed())
        return {XrdCl::stError, XrdCl::errFcntl};

    std::size_t size = op_stat->GetStatInfo().first;

    std::shared_ptr<CurlCopyOp> op_copy(new CurlCopyOp(nullptr, GetSource().GetURL(), {}, GetTarget().GetURL(), {}, {10, 0}, nullptr, nullptr));
    op_copy->SetCallback([this, progress, size] (auto bytemark)
        {
            progress->JobProgress(this->pJobId, bytemark, size);
        });

    try {
        m_queue->Produce(op_copy);
    } catch (...) {
        log->Warning(kLogXrdClHttp, "Failed to add copy op to queue");
        return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errOSError);
    }

    while (!op_copy->IsDone())
        std::this_thread::sleep_for(std::chrono::milliseconds(250));

    if (!op_copy->IsSentSucessfully())
        return {XrdCl::stError, XrdCl::errPipelineFailed, 0, op_copy->GetSendingFailureMessage()};

    progress->JobProgress(pJobId, size, size);

    return XrdCl::XRootDStatus();
}
