#ifndef XRDCLHTTPTHIRDPARTYCOPYJOB_HH
#define XRDCLHTTPTHIRDPARTYCOPYJOB_HH

#include "XrdClHttpFilesystem.hh"

namespace XrdClHttp {

class ThirdPartyCopy : public XrdCl::CopyJob
{
private:
    std::shared_ptr<XrdClHttp::HandlerQueue> m_queue;
    CreateConnCalloutType callout;

public:
    ThirdPartyCopy(uint32_t      jobId,
                    XrdCl::PropertyList *jobProperties,
                    XrdCl::PropertyList *jobResults,
                    std::shared_ptr<XrdClHttp::HandlerQueue> queue,
                    CreateConnCalloutType callout);
    virtual ~ThirdPartyCopy() = default;
    virtual XrdCl::XRootDStatus Run(XrdCl::CopyProgressHandler *progress = 0) override;
};

}

#endif
