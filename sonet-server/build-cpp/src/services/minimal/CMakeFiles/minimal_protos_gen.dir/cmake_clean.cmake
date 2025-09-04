file(REMOVE_RECURSE
  "CMakeFiles/minimal_protos_gen"
  "gen/common/common.grpc.pb.cc"
  "gen/common/common.grpc.pb.h"
  "gen/common/common.pb.cc"
  "gen/common/common.pb.h"
  "gen/common/pagination.grpc.pb.cc"
  "gen/common/pagination.grpc.pb.h"
  "gen/common/pagination.pb.cc"
  "gen/common/pagination.pb.h"
  "gen/common/timestamp.grpc.pb.cc"
  "gen/common/timestamp.grpc.pb.h"
  "gen/common/timestamp.pb.cc"
  "gen/common/timestamp.pb.h"
  "gen/services/notification.grpc.pb.cc"
  "gen/services/notification.grpc.pb.h"
  "gen/services/notification.pb.cc"
  "gen/services/notification.pb.h"
  "gen/services/search.grpc.pb.cc"
  "gen/services/search.grpc.pb.h"
  "gen/services/search.pb.cc"
  "gen/services/search.pb.h"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/minimal_protos_gen.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
