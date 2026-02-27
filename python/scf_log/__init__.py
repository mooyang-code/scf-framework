"""
scf_log — SCF 框架 Python CLS 日志上报模块。

提供与 trpc-go-log-cls 功能对等的 Python 实现：
  - 异步批量上报到腾讯云 CLS
  - 从 YAML 配置读取 CLS 连接参数
  - 集成 Python 标准 logging 模块

快速使用::

    from scf_log import setup_cls_logging

    # 方式一：从 config.yaml 的 plugin.cls 节点读取配置
    setup_cls_logging(config_path="./configs/config.yaml")

    # 方式二：直接传字典
    setup_cls_logging(config_dict={
        "topic_id": "xxx",
        "host": "ap-guangzhou.cls.tencentcs.com",
        "secret_id": "AKIDxxx",
        "secret_key": "xxx",
    })

    import logging
    logger = logging.getLogger("my-service")
    logger.info("hello CLS")
    logger.info("with extra", extra={"request_id": "abc123"})
"""

from __future__ import annotations

import logging
from typing import Optional

from .config import CLSConfig
from .handler import CLSHandler
from .producer import AsyncProducer

__all__ = [
    "CLSConfig",
    "CLSHandler",
    "AsyncProducer",
    "setup_cls_logging",
]


def setup_cls_logging(
    config_path: Optional[str] = None,
    config_dict: Optional[dict] = None,
    config: Optional[CLSConfig] = None,
    level: int = logging.DEBUG,
    logger_name: Optional[str] = None,
    cls_key: str = "cls",
    time_format: str = "%Y-%m-%d %H:%M:%S",
) -> CLSHandler:
    """一行代码接入 CLS 日志上报。

    三种配置来源（按优先级）：
      1. 直接传入 ``config`` 对象
      2. 传入 ``config_dict`` 字典
      3. 传入 ``config_path`` YAML 文件路径

    Args:
        config_path:  YAML 配置文件路径
        config_dict:  配置字典
        config:       CLSConfig 对象
        level:        日志级别阈值
        logger_name:  目标 logger 名称，None 表示 root logger
        cls_key:      YAML 中 plugin 节点下的 key，默认 "cls"
        time_format:  日志时间格式

    Returns:
        创建的 CLSHandler 实例

    Raises:
        ValueError: 配置不完整或未提供任何配置来源
    """
    # 解析配置
    if config is not None:
        cls_config = config
    elif config_dict is not None:
        cls_config = CLSConfig.from_dict(config_dict)
    elif config_path is not None:
        cls_config = CLSConfig.from_yaml(config_path, key=cls_key)
    else:
        raise ValueError(
            "必须提供 config_path、config_dict 或 config 中的至少一个"
        )

    cls_config.validate()

    # 创建 handler
    handler = CLSHandler(cls_config, level=level, time_format=time_format)

    # 设置格式器（简洁格式，详细信息已在 fields 中）
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    # 添加到 logger
    target_logger = logging.getLogger(logger_name)
    target_logger.addHandler(handler)

    # 确保 logger 级别不高于 handler 级别
    if target_logger.level == logging.NOTSET or target_logger.level > level:
        target_logger.setLevel(level)

    return handler
