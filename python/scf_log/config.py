"""
CLS 日志配置模块。

从 YAML 配置文件或字典中解析 CLS 连接参数，对应 trpc-go-log-cls 的 Config 结构。
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CLSConfig:
    """CLS 日志上报配置。

    必填字段:
        topic_id:   CLS 日志主题 ID
        host:       CLS 上报域名，如 "ap-guangzhou.cls.tencentcs.com"
        secret_id:  腾讯云 SecretID
        secret_key: 腾讯云 SecretKey

    可选字段参考 trpc-go-log-cls 的默认值。
    """

    # ---- 必填 ----
    topic_id: str = ""
    host: str = ""
    secret_id: str = ""
    secret_key: str = ""

    # ---- 可选（带默认值，与 Go 版一致） ----
    source: str = ""                          # 日志来源 IP，空则自动获取本机 IP
    total_size_bytes: int = 104857600         # 缓存上限 100MB
    max_send_workers: int = 50                # 最大并发发送线程数
    max_block_sec: int = 0                    # 缓冲区满时阻塞秒数（0=非阻塞，丢弃）
    max_batch_size: int = 5242880             # 批量大小阈值 5MB
    max_batch_count: int = 4096               # 批量条数阈值
    linger_ms: int = 2000                     # 批量等待时间（毫秒）
    retries: int = 10                         # 失败重试次数
    base_retry_backoff_ms: int = 100          # 首次重试退避（毫秒）
    max_retry_backoff_ms: int = 50000         # 最大重试退避（毫秒）
    field_map: Optional[dict[str, str]] = None  # 字段名映射

    def __post_init__(self):
        if not self.source:
            self.source = _get_local_ip()

    def validate(self) -> None:
        """校验必填字段。"""
        missing = []
        if not self.topic_id:
            missing.append("topic_id")
        if not self.host:
            missing.append("host")
        if not self.secret_id:
            missing.append("secret_id")
        if not self.secret_key:
            missing.append("secret_key")
        if missing:
            raise ValueError(f"CLS 配置缺少必填字段: {', '.join(missing)}")

    # ------------------------------------------------------------------ #
    # 工厂方法
    # ------------------------------------------------------------------ #

    @classmethod
    def from_dict(cls, d: dict) -> CLSConfig:
        """从字典创建配置（兼容 YAML 中 snake_case 风格的 key）。"""
        # 字段映射：YAML key → dataclass 字段名
        _alias = {
            "total_size_ln_bytes": "total_size_bytes",
            "max_send_worker_count": "max_send_workers",
        }
        mapped = {}
        for k, v in d.items():
            field_name = _alias.get(k, k)
            mapped[field_name] = v

        # 只保留 dataclass 中存在的字段
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in mapped.items() if k in valid_fields}
        return cls(**filtered)

    @classmethod
    def from_yaml(cls, path: str, key: str = "cls") -> CLSConfig:
        """从 YAML 文件读取配置。

        支持两种格式:
          1. 顶层直接是 CLS 配置字段
          2. 嵌套在 ``plugin.<key>`` 下（scf-framework config.yaml 格式）

        Args:
            path: YAML 文件路径
            key:  plugin 节点下的子 key，默认 "cls"
        """
        import yaml

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            raise ValueError(f"YAML 文件格式错误: {path}")

        # 尝试从 plugin.<key> 读取
        plugin_node = data.get("plugin")
        if isinstance(plugin_node, dict) and key in plugin_node:
            return cls.from_dict(plugin_node[key])

        # 尝试顶层直接读取（独立的 cls 配置文件）
        if "topic_id" in data:
            return cls.from_dict(data)

        # 如果指定 key 就在顶层
        if key in data:
            return cls.from_dict(data[key])

        raise ValueError(
            f"无法从 {path} 中解析 CLS 配置，"
            f"请确保配置在顶层或 plugin.{key} 下"
        )


def _get_local_ip() -> str:
    """获取本机 IP 地址。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
