#pragma once

#include <string>
#include "communicator.h"

/**
 * @brief 服务端启动参数
 * @ingroup Communicator
 */
class ServerParam 
{
public:
  ServerParam();

  ServerParam(const ServerParam &other) = default;
  ~ServerParam() = default;

public:
  // accpet client's address, default is INADDR_ANY, means accept every address
  long listen_addr;

  int max_connection_num;  ///< 最大连接数

  int port; ///< 监听的端口号

  std::string unix_socket_path; ///< unix socket的路径

  bool use_std_io = false;  ///< 是否使用标准输入输出作为通信条件

  ///< 如果使用标准输入输出作为通信条件，就不再监听端口
  ///< 后面如果改成支持多种通讯方式，就不需要这个参数了
  bool use_unix_socket = false;

  CommunicateProtocol protocol; ///< 通讯协议，目前支持文本协议和mysql协议
};
