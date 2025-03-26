# -*- coding:utf-8 -*-
# ===============================================
# 文件描述：
# 作   者：HUP
# 日   期: 2025/3/25
# 邮   箱: 28971609@qq.com
# ===============================================

import logging
import os
import sys
from loguru import logger


class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def set_logger(name=None, log_path: str = None, level="INFO", format: str = None):
    # 清空logging 模块的默认处理器并设置新的处理器
    logging.basicConfig(handlers=[InterceptHandler()], level=level)
    # 移除默认的日志处理器
    logger.remove()
    # 拼接输出日志文件，未配置日志打印到终端
    if log_path:
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        sink = os.path.join(log_path, f"{name}.log")
    else:
        sink = sys.stdout
    _format = format or "[{time:YYYY-MM-DD HH:mm:ss} {level}] {file.name}:{line} {message}"
    logger.add(sink, format=_format, level=level)
