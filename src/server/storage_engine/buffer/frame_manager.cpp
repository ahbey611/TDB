#include "include/storage_engine/buffer/frame_manager.h"

FrameManager::FrameManager(const char *tag) : allocator_(tag)
{}

RC FrameManager::init(int pool_num)
{
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    return RC::SUCCESS;
  }
  return RC::NOMEM;
}

RC FrameManager::cleanup()
{
  if (frames_.count() > 0) {
    return RC::INTERNAL;
  }
  frames_.destroy();
  return RC::SUCCESS;
}

Frame *FrameManager::alloc(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    return frame;
  }

  frame = allocator_.alloc();
  if (frame != nullptr) {
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s",
        to_string(*frame).c_str());
    frame->set_page_num(page_num);
    frame->pin();
    frames_.put(frame_id, frame);
  }
  return frame;
}

Frame *FrameManager::get(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  return get_internal(frame_id);
}

/**
 * TODO [Lab1] 需要同学们实现页帧驱逐
 */
int FrameManager::evict_frames(int count, std::function<RC(Frame *frame)> evict_action)
{
  // 刷盘场景1：内存缓冲区已满，需要进行页面置换
  /*
   * Frame是磁盘文件的每个页面在内存中的存储方式，也是数据持久化的基本单位，在内存中由FrameLruCache管理。
   * 但FrameLruCache容量是有限的，当已分配的Frame数量达到阈值，
   * 但仍需要申请新的Frame来存储新Page的数据时，就需要进行页帧替换，
   * 将pin count=0的Frame驱逐出去。还要注意如果这些Frame对应的Page是脏页（即发生过数据修改），
   * 就需要先刷回磁盘再驱逐。
   */

  /*
   * 提示
   * 1. 该函数的作用是将pin_count_为0的frame从队列中驱逐
   * 2. 参数evict_action表示如何处理脏数据的frame，一般是要刷回磁盘，同学们可以参考FileBufferPool::allocate_frame方法中的相关逻辑
   * 3. 对于FrameLruCache的使用，要通过加锁保证线程安全，但注意避免死锁
   * 4. 需要调用FrameAllocator的接口彻底释放该Frame
   */

    std::lock_guard<std::mutex> lock_guard(lock_);
    std::list<Frame *> frames;
    auto fetcher = [&frames](const FrameId &frame_id, Frame *const frame) -> bool {
        if (frame->pin_count() == 0) {
            frame->pin();
            frames.push_back(frame);
        }
        return true;
    };

    int evict_count = 0;

    frames_.foreach(fetcher);
    for (auto frame : frames) {
        if (count <= 0) {
            break;
        }
        if (frame->dirty()) {
            evict_action(frame);
        }
        frames_.remove(frame->frame_id());
        allocator_.free(frame);
        count--;
        evict_count++;
    }

    return evict_count;

}

Frame *FrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  (void)frames_.get(frame_id, frame);
  if (frame != nullptr) {
    frame->pin();
  }
  return frame;
}

/**
 * @brief 查找目标文件的frame
 * FramesCache中选出所有与给定文件描述符(file_desc)相匹配的Frame对象，并将它们添加到列表中
 */
std::list<Frame *> FrameManager::find_list(int file_desc)
{
  std::lock_guard<std::mutex> lock_guard(lock_);

  std::list<Frame *> frames;
  auto fetcher = [&frames, file_desc](const FrameId &frame_id, Frame *const frame) -> bool {
    if (file_desc == frame_id.file_desc()) {
      frame->pin();
      frames.push_back(frame);
    }
    return true;
  };
  frames_.foreach (fetcher);
  return frames;
}