"""
CLS 日志 Handler — 集成到 Python 标准 logging 模块。

将 logging.LogRecord 转换为 CLS 日志字段，通过 AsyncProducer 异步批量上报。
对应 trpc-go-log-cls 中 cls.Logger.Write() 的功能。
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Optional

from .config import CLSConfig
from .producer import AsyncProducer


class CLSHandler(logging.Handler):
    """Python logging.Handler 实现，将日志异步上报到腾讯云 CLS。

    用法::

        config = CLSConfig(topic_id="xxx", host="xxx", secret_id="xxx", secret_key="xxx")
        handler = CLSHandler(config, level=logging.INFO)
        logging.getLogger().addHandler(handler)

    字段映射（与 trpc-go-log-cls 保持一致）:
        - Time:       日志时间 "2006-01-02 15:04:05" 格式
        - Level:      日志级别（INFO / WARNING / ERROR 等）
        - Msg:        日志消息
        - Caller:     调用位置 "filename:lineno"
        - Name:       logger 名称
        - StackTrace: 异常堆栈（仅在有异常时）

    额外字段:
        通过 ``logging.info("msg", extra={"key": "value"})`` 传入的字段
        会自动添加到 CLS 日志内容中。
    """

    # 与 trpc-go-log-cls 一致的字段名
    TIME_KEY = "Time"
    LEVEL_KEY = "Level"
    MSG_KEY = "Msg"
    CALLER_KEY = "Caller"
    FUNC_KEY = "func"
    NAME_KEY = "Name"
    STACKTRACE_KEY = "StackTrace"

    # logging.LogRecord 自带的属性名（用于过滤 extra 字段）
    _BUILTIN_ATTRS = frozenset({
        "name", "msg", "args", "created", "relativeCreated", "exc_info",
        "exc_text", "stack_info", "lineno", "funcName", "filename",
        "module", "pathname", "thread", "threadName", "process",
        "processName", "levelname", "levelno", "msecs", "message",
        "taskName",
    })

    def __init__(
        self,
        config: CLSConfig,
        level: int = logging.DEBUG,
        time_format: str = "%Y-%m-%d %H:%M:%S",
    ):
        """
        Args:
            config:      CLS 配置
            level:       日志级别阈值
            time_format: 日志时间格式（与 trpc-go-log-cls 保持一致）
        """
        super().__init__(level)
        self._config = config
        self._time_format = time_format
        self._producer = AsyncProducer(config)
        self._context_fields: dict[str, str] = {}

    def set_context_fields(self, **kwargs) -> None:
        """设置全局上下文字段，会自动注入到每条日志中。

        用法::

            handler.set_context_fields(nodeID="scfxxx-xxx", version="v0.0.8")
        """
        for k, v in kwargs.items():
            self._context_fields[k] = str(v)

    @property
    def producer(self) -> AsyncProducer:
        """返回内部的 AsyncProducer 实例（可用于查看统计信息）。"""
        return self._producer

    def emit(self, record: logging.LogRecord) -> None:
        """处理一条日志记录。"""
        try:
            fields = self._record_to_fields(record)
            timestamp_us = int(record.created * 1_000_000)
            self._producer.send(fields, timestamp_us=timestamp_us)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        """关闭 handler，flush 剩余日志。"""
        try:
            self._producer.close()
        except Exception:
            pass
        super().close()

    def _record_to_fields(self, record: logging.LogRecord) -> dict[str, str]:
        """将 LogRecord 转换为 CLS 字段 dict。"""
        # 格式化消息
        msg = self.format(record)

        # 格式化时间
        log_time = datetime.fromtimestamp(record.created).strftime(self._time_format)

        fields: dict[str, str] = {
            self.TIME_KEY: log_time,
            self.LEVEL_KEY: record.levelname,
            self.MSG_KEY: msg,
            self.CALLER_KEY: f"{record.filename}:{record.lineno}",
            self.FUNC_KEY: record.funcName or "",
            self.NAME_KEY: record.name or "",
        }

        # 注入全局上下文字段（nodeID, version 等）
        for key, value in self._context_fields.items():
            if key not in fields:
                fields[key] = value

        # 异常堆栈
        if record.exc_info and record.exc_info[0] is not None:
            tb = "".join(traceback.format_exception(*record.exc_info))
            fields[self.STACKTRACE_KEY] = tb

        # 提取 extra 字段（用户通过 extra={} 传入的自定义字段）
        for key, value in record.__dict__.items():
            if key not in self._BUILTIN_ATTRS and key not in fields:
                fields[key] = str(value)

        # 应用 field_map（与 trpc-go-log-cls 的 GetReportCLSField 对应）
        if self._config.field_map:
            fields = self._apply_field_map(fields)

        return fields

    def _apply_field_map(self, fields: dict[str, str]) -> dict[str, str]:
        """应用字段名映射。"""
        field_map = self._config.field_map
        if not field_map:
            return fields

        mapped: dict[str, str] = {}
        for key, value in fields.items():
            # 如果在映射表中，使用映射后的名称
            report_key = field_map.get(key, key)
            mapped[report_key] = value

        return mapped
