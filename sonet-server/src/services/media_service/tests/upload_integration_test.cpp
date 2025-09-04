#include <cassert>
#include "../../../../proto/grpc_stub.h"
#include <thread>
#include <filesystem>
#include <fstream>
#include "../service.h"

using namespace sonet::media_service;
namespace fs = std::filesystem;

static std::unique_ptr<grpc::Server> StartServer(MediaServiceImpl* svc, const std::string& addr) {
    grpc::ServerBuilder b; b.AddListeningPort(addr, grpc::InsecureServerCredentials()); b.RegisterService(svc); return b.BuildAndStart();
}

int run_upload_test() {
    auto repo = std::shared_ptr<MediaRepository>(CreateInMemoryRepo().release());
    auto storage = std::shared_ptr<StorageBackend>(CreateLocalStorage("/tmp/sonet-media-test", "file:///tmp/sonet-media-test").release());
    auto img = std::shared_ptr<ImageProcessor>(CreateImageProcessor().release());
    auto vid = std::shared_ptr<VideoProcessor>(CreateVideoProcessor().release());
    auto gif = std::shared_ptr<GifProcessor>(CreateGifProcessor().release());
    auto nsfw = std::shared_ptr<NsfwScanner>(CreateBasicScanner(false).release());
    MediaServiceImpl svc(repo, storage, img, vid, gif, nsfw, 5 * 1024 * 1024);
    auto server = StartServer(&svc, "127.0.0.1:56051");
    assert(server);
    fs::create_directories("/tmp/sonet-media-test");
    fs::path png = fs::temp_directory_path() / "tiny_test.png";
    {
        const unsigned char bytes[] = {0x89,'P','N','G',0x0D,0x0A,0x1A,0x0A,0,0,0,0};
        std::ofstream ofs(png, std::ios::binary); ofs.write((const char*)bytes, sizeof(bytes));
    }
    auto channel = grpc::CreateChannel("127.0.0.1:56051", grpc::InsecureChannelCredentials());
    auto stub = std::make_unique<::sonet::media::MediaService::Stub>(channel);
    grpc::ClientContext ctx; ctx.AddMetadata("x-user-id", "u1");
    ::sonet::media::UploadResponse resp;
    auto writer = stub->Upload(&ctx, &resp);
    ::sonet::media::UploadRequest init;
    auto* i = init.mutable_init(); i->set_owner_user_id("u1"); i->set_type(::sonet::media::MEDIA_TYPE_IMAGE); i->set_mime_type("image/png");
    bool wrote = writer->Write(init); assert(wrote);
    std::ifstream ifs(png, std::ios::binary); std::vector<char> buf((std::istreambuf_iterator<char>(ifs)), {});
    ::sonet::media::UploadRequest chunk; chunk.mutable_chunk()->set_content(std::string(buf.begin(), buf.end()));
    writer->Write(chunk); writer->WritesDone();
    auto status = writer->Finish();
    assert(status.ok());
    assert(!resp.media_id().empty());
    server->Shutdown();
    return 0;
}

// No main here; main defined in media_service_unit_tests.cpp will call run_upload_test().
