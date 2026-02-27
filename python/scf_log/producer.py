"""
异步批量日志生产者。

参考 Go CLS SDK 的 AsyncProducerClient 设计，在 Python 侧实现：
  - 后台 daemon 线程定时 flush
  - 基于条数 / 大小 / 超时三重阈值触发批量发送
  - 发送失败时指数退避重试
  - 内存缓冲区大小限制
  - 优雅关闭（flush 剩余日志）
"""

from __future__ import annotations

import atexit
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from tencentcloud.log.logclient import LogClient
from tencentcloud.log.cls_pb2 import LogGroupList
from tencentcloud.log.logexception import LogException

from .config import CLSConfig

_logger = logging.getLogger("scf_log.producer")


# ============================================================================
# 内部数据结构
# ============================================================================


@dataclass
class LogEntry:
    """单条日志条目。"""

    timestamp_us: int                   # 微秒时间戳
    fields: dict[str, str]              # key-value 日志内容
    size: int = 0                       # 估算字节大小

    def __post_init__(self):
        if self.size == 0:
            self.size = sum(
                len(k) + len(v) for k, v in self.fields.items()
            )


@dataclass
class _Batch:
    """一个待发送的日志批次。"""

    entries: list[LogEntry] = field(default_factory=list)
    total_size: int = 0
    count: int = 0

    def add(self, entry: LogEntry) -> None:
        self.entries.append(entry)
        self.total_size += entry.size
        self.count += 1

    def is_empty(self) -> bool:
        return self.count == 0


# ============================================================================
# 统计计数
# ============================================================================


class ProducerStats:
    """生产者运行统计（线程安全）。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.send_success: int = 0
        self.send_fail: int = 0
        self.log_count: int = 0
        self.drop_count: int = 0

    def incr_success(self) -> None:
        with self._lock:
            self.send_success += 1

    def incr_fail(self) -> None:
        with self._lock:
            self.send_fail += 1

    def incr_logs(self, n: int) -> None:
        with self._lock:
            self.log_count += n

    def incr_drop(self, n: int) -> None:
        with self._lock:
            self.drop_count += n

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "send_success": self.send_success,
                "send_fail": self.send_fail,
                "log_count": self.log_count,
                "drop_count": self.drop_count,
            }


# ============================================================================
# AsyncProducer
# ============================================================================


class AsyncProducer:
    """异步批量日志生产者。

    用法::

        producer = AsyncProducer(config)
        producer.send({"level": "INFO", "msg": "hello"})
        ...
        producer.close()   # 程序退出前调用
    """

    def __init__(self, config: CLSConfig):
        config.validate()
        self._config = config
        self._stats = ProducerStats()

        # CLS SDK 客户端
        endpoint = config.host
        if not endpoint.startswith("http"):
            endpoint = "https://" + endpoint
        self._client = LogClient(
            endpoint,
            config.secret_id,
            config.secret_key,
            source=config.source,
        )

        # 缓冲区
        self._buffer: list[LogEntry] = []
        self._buffer_size: int = 0            # 当前缓冲字节数
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

        # 控制
        self._closed = False
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name="scf-cls-flush",
            daemon=True,
        )
        self._flush_thread.start()

        # 注册 atexit 确保程序退出时 flush
        atexit.register(self._atexit_flush)

    # ------------------------------------------------------------------ #
    # 公共 API
    # ------------------------------------------------------------------ #

    def send(
        self,
        log_fields: dict[str, str],
        timestamp_us: Optional[int] = None,
    ) -> bool:
        """发送一条日志。

        Args:
            log_fields:   日志字段 key-value
            timestamp_us: 微秒时间戳，None 则使用当前时间

        Returns:
            True 表示成功加入缓冲区，False 表示被丢弃（缓冲区满且非阻塞模式）
        """
        if self._closed:
            return False

        if timestamp_us is None:
            timestamp_us = int(time.time() * 1_000_000)

        entry = LogEntry(timestamp_us=timestamp_us, fields=log_fields)

        with self._not_full:
            # 检查缓冲区是否已满
            if self._buffer_size + entry.size > self._config.total_size_bytes:
                if self._config.max_block_sec <= 0:
                    # 非阻塞：丢弃
                    self._stats.incr_drop(1)
                    _logger.warning(
                        "CLS 日志缓冲区已满，丢弃日志 (buffer_size=%d, limit=%d)",
                        self._buffer_size, self._config.total_size_bytes,
                    )
                    return False
                else:
                    # 阻塞等待
                    deadline = time.monotonic() + self._config.max_block_sec
                    while self._buffer_size + entry.size > self._config.total_size_bytes:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._stats.incr_drop(1)
                            _logger.warning("CLS 日志缓冲区等待超时，丢弃日志")
                            return False
                        self._not_full.wait(timeout=remaining)

            self._buffer.append(entry)
            self._buffer_size += entry.size

            # 如果达到批量阈值，立即通知 flush 线程
            if (self._buffer_size >= self._config.max_batch_size
                    or len(self._buffer) >= self._config.max_batch_count):
                self._not_empty.notify()

        return True

    def flush(self) -> None:
        """立即发送缓冲区中的所有日志。"""
        entries = self._drain_buffer()
        if entries:
            self._send_batch(entries)

    def close(self, timeout_ms: int = 60000) -> None:
        """优雅关闭：flush 剩余日志并等待发送完成。

        Args:
            timeout_ms: 等待超时（毫秒）
        """
        if self._closed:
            return
        self._closed = True

        # 唤醒 flush 线程使其退出
        with self._not_empty:
            self._not_empty.notify()

        # 等待 flush 线程结束
        self._flush_thread.join(timeout=timeout_ms / 1000)

        # 发送剩余日志
        self.flush()

        stats = self._stats.snapshot()
        _logger.info(
            "CLS AsyncProducer 已关闭: success=%d, fail=%d, logs=%d, drop=%d",
            stats["send_success"], stats["send_fail"],
            stats["log_count"], stats["drop_count"],
        )

    @property
    def stats(self) -> dict:
        """返回运行统计快照。"""
        return self._stats.snapshot()

    # ------------------------------------------------------------------ #
    # 内部方法
    # ------------------------------------------------------------------ #

    def _flush_loop(self) -> None:
        """后台 flush 线程主循环。"""
        linger_sec = self._config.linger_ms / 1000.0

        while not self._closed:
            with self._not_empty:
                # 等待至超时或被通知
                self._not_empty.wait(timeout=linger_sec)

            # 取出缓冲区
            entries = self._drain_buffer()
            if entries:
                self._send_batch(entries)

    def _drain_buffer(self) -> list[LogEntry]:
        """取出并清空缓冲区，返回所有待发送日志。"""
        with self._lock:
            if not self._buffer:
                return []
            entries = self._buffer
            self._buffer = []
            self._buffer_size = 0

        # 通知等待的 send() 线程
        with self._not_full:
            self._not_full.notify_all()

        return entries

    def _send_batch(self, entries: list[LogEntry]) -> None:
        """将一批日志发送到 CLS，失败时指数退避重试。"""
        # 按 max_batch_count 分块发送
        for i in range(0, len(entries), self._config.max_batch_count):
            chunk = entries[i : i + self._config.max_batch_count]
            self._send_chunk(chunk)

    def _send_chunk(self, entries: list[LogEntry]) -> None:
        """发送一个分块。"""
        log_group_list = self._build_log_group_list(entries)

        backoff_ms = self._config.base_retry_backoff_ms
        last_err: Optional[Exception] = None

        for attempt in range(self._config.retries + 1):
            try:
                self._client.put_log_raw(self._config.topic_id, log_group_list)
                self._stats.incr_success()
                self._stats.incr_logs(len(entries))
                return
            except LogException as e:
                last_err = e
                error_code = e.get_error_code() if hasattr(e, "get_error_code") else ""
                # 可重试的错误
                retryable = error_code in (
                    "InternalError", "Timeout", "SpeedQuotaExceed",
                ) or (hasattr(e, "resp_status") and e.resp_status >= 500)

                if not retryable or attempt >= self._config.retries:
                    break

                _logger.warning(
                    "CLS 发送失败，重试 %d/%d: code=%s, msg=%s",
                    attempt + 1, self._config.retries,
                    error_code, str(e),
                )
                time.sleep(backoff_ms / 1000.0)
                backoff_ms = min(backoff_ms * 2, self._config.max_retry_backoff_ms)

            except Exception as e:
                last_err = e
                if attempt >= self._config.retries:
                    break
                _logger.warning(
                    "CLS 发送异常，重试 %d/%d: %s",
                    attempt + 1, self._config.retries, e,
                )
                time.sleep(backoff_ms / 1000.0)
                backoff_ms = min(backoff_ms * 2, self._config.max_retry_backoff_ms)

        # 全部重试失败
        self._stats.incr_fail()
        _logger.error(
            "CLS 发送最终失败，丢弃 %d 条日志: %s",
            len(entries), last_err,
        )

    def _build_log_group_list(self, entries: list[LogEntry]) -> LogGroupList:
        """构建 CLS protobuf LogGroupList。"""
        log_group_list = LogGroupList()
        log_group = log_group_list.logGroupList.add()
        log_group.source = self._config.source

        field_map = self._config.field_map

        for entry in entries:
            log = log_group.logs.add()
            log.time = entry.timestamp_us

            for key, value in entry.fields.items():
                # 应用字段映射
                report_key = key
                if field_map and key in field_map:
                    report_key = field_map[key]

                content = log.contents.add()
                content.key = report_key
                content.value = str(value)

        return log_group_list

    def _atexit_flush(self) -> None:
        """atexit 回调，确保程序退出时发送残留日志。"""
        if not self._closed:
            try:
                self.close(timeout_ms=5000)
            except Exception:
                pass
