cmake_minimum_required(VERSION 3.20)
project(worker_service LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

set(CROW_BUILD_EXAMPLES OFF)
set(CROW_BUILD_TESTS OFF)
set(CROW_BUILD_DOCS OFF)
set(CROW_USE_BOOST OFF)

include(FetchContent)
FetchContent_Declare(
  Crow
  GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
  GIT_TAG v1.1.1
)
FetchContent_MakeAvailable(Crow)

find_package(PkgConfig REQUIRED)
pkg_check_modules(FFMPEG REQUIRED libavcodec libavformat libavfilter libavutil libswscale libswresample)
find_package(CUDAToolkit REQUIRED)
find_package(CURL REQUIRED)

add_executable(worker_service
  src/worker_main.cpp
  src/cuda_kernels.cu
)

target_include_directories(worker_service PRIVATE
  ${FFMPEG_INCLUDE_DIRS}
  ${CUDAToolkit_INCLUDE_DIRS}
)

target_link_libraries(worker_service PRIVATE
  ${FFMPEG_LIBRARIES}
  Crow::Crow
  CUDA::cudart
  CURL::libcurl
)

if (UNIX)
  target_link_libraries(worker_service PRIVATE pthread dl m)
endif()
