"""
scf_log 使用示例。

演示三种接入 CLS 日志上报的方式。
"""

import logging
import time

# ============================================================================
# 方式一：从 config.yaml 的 plugin.cls 节点读取配置（推荐）
# ============================================================================
#
# 配置文件格式（config.yaml）：
#
#   plugin:
#     cls:
#       topic_id: "your-topic-id"
#       host: "ap-guangzhou.cls.tencentcs.com"
#       secret_id: "AKIDxxxx"
#       secret_key: "xxxx"
#       source: "127.0.0.1"
#       max_batch_count: 4096
#       linger_ms: 2000
#       retries: 10
#
# 代码：
#
#   from scf_log import setup_cls_logging
#   handler = setup_cls_logging(config_path="./configs/config.yaml")
#
# 之后所有日志自动上报到 CLS：
#   logger = logging.getLogger("my-service")
#   logger.info("started")
#   logger.error("something failed", extra={"request_id": "abc123"})


# ============================================================================
# 方式二：直接传字典
# ============================================================================
#
#   from scf_log import setup_cls_logging
#   handler = setup_cls_logging(config_dict={
#       "topic_id": "your-topic-id",
#       "host": "ap-guangzhou.cls.tencentcs.com",
#       "secret_id": "AKIDxxxx",
#       "secret_key": "xxxx",
#   })


# ============================================================================
# 方式三：手动创建（适合精细控制场景）
# ============================================================================


def example_manual():
    """手动创建 CLSConfig + CLSHandler 的示例。"""
    from scf_log import CLSConfig, CLSHandler

    # 1. 创建配置
    config = CLSConfig(
        topic_id="your-topic-id",
        host="ap-guangzhou.cls.tencentcs.com",
        secret_id="AKIDxxxx",
        secret_key="xxxx",
        source="127.0.0.1",
        max_batch_count=4096,
        linger_ms=2000,
        retries=10,
        field_map={
            "Level": "log_level",    # 将 Level 字段映射为 log_level
            "Msg": "message",        # 将 Msg 字段映射为 message
        },
    )

    # 2. 创建 handler
    handler = CLSHandler(config, level=logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))

    # 3. 添加到 logger
    logger = logging.getLogger("my-service")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # 4. 使用 - 日志自动上报到 CLS
    logger.info("服务启动")
    logger.warning("磁盘空间不足", extra={"disk_usage": "95%"})

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("计算错误")  # 异常堆栈会自动上报

    # 5. 查看统计
    print(f"发送统计: {handler.producer.stats}")

    # 6. 程序退出前关闭（flush 剩余日志）
    handler.close()


# ============================================================================
# 方式四：直接使用 AsyncProducer（不走 logging 模块）
# ============================================================================


def example_raw_producer():
    """直接使用 AsyncProducer 发送结构化日志。"""
    from scf_log import CLSConfig, AsyncProducer

    config = CLSConfig(
        topic_id="your-topic-id",
        host="ap-guangzhou.cls.tencentcs.com",
        secret_id="AKIDxxxx",
        secret_key="xxxx",
    )

    producer = AsyncProducer(config)

    # 发送结构化日志
    for i in range(100):
        producer.send({
            "level": "INFO",
            "module": "data-collector",
            "message": f"采集第 {i} 条数据",
            "symbol": "BTC-USDT",
            "interval": "1m",
        })

    # 查看统计
    print(f"发送统计: {producer.stats}")

    # 关闭（等待所有日志发送完成）
    producer.close(timeout_ms=10000)


if __name__ == "__main__":
    # 运行前请替换配置中的 topic_id / secret_id / secret_key
    print("请取消注释相应的示例函数来运行")
    # example_manual()
    # example_raw_producer()
