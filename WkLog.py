# -*- coding: utf-8 -*-
# @Date     : 2023-10-12 17:20:00
# @Author   : WangKang
# @Blog     : kang17.xyz
# @Email    : 1686617586@qq.com
# @Filepath : WkLog.py
# @Brief    : 日志类，仿照springboot编写的简化版
# Copyright 2023 WANGKANG, All Rights Reserved.

""" 
默认配置:
    0. 时间格式: %Y-%m-%d %H:%M:%S.%f
    1. 文件输出关闭
    2. 单文件最大尺寸: 10MB
    3. 默认输出路径: ./log
"""

# 使用colorama重写

import os
from datetime import datetime
import inspect
from colorama import Fore, Back, Style, init


class MyLog:
    LEVEL_COLOR = {
        "Debug": Fore.BLUE,
        "Info": Fore.GREEN,
        "Warn": Fore.YELLOW,
        "Error": Fore.RED,
    }
    TIME_COLOR = Fore.CYAN
    NORMAL_COLOR = Fore.RESET

    DEBUG = "Debug"
    INFO = "Info"
    WARN = "Warn"
    ERROR = "Error"

    MODE = ["Debug", "Info", "Warn", "Error"]

    SLIENT = False  # 静默模式 开启后将隐藏重要的日志信息，避免用户看到重要信息

    def __init__(self):
        self.mode = "Debug"
        self.time_format = "%Y-%m-%d %H:%M:%S.%f"
        self.output_to_file = False  # 是否输出到文件
        self.dir_path = "./log"  # 默认输出文件路径
        self.file_archive = False  # 文件归档
        self.rolling_cutting = False  # 滚动切割
        self.file_max_size = 10 * 1024  # 单位kb
        self.rolling_cutting_index = 1  # 用来记录当前归档序号
        self.clear_pre_output = False  # 是否清空之前的日志输出
        self.init_settings()
        init(autoreset=True)

    def Debug(self, msg):
        if self.mode in self.MODE[1:]:
            return
        class_name = self.get_calling_class_name()
        func_name = self.get_calling_func_name()
        level = "Debug"
        self.print_msg(class_name, func_name, level, msg)

    def Info(self, msg):
        if self.mode in self.MODE[2:]:
            return
        class_name = self.get_calling_class_name()
        func_name = self.get_calling_func_name()
        level = "Info"
        self.print_msg(class_name, func_name, level, msg)

    def Warn(self, msg):
        if self.mode in self.MODE[3:]:
            return
        class_name = self.get_calling_class_name()
        func_name = self.get_calling_func_name()
        level = "Warn"
        self.print_msg(class_name, func_name, level, msg)

    def Error(self, msg):
        class_name = self.get_calling_class_name()
        func_name = self.get_calling_func_name()
        level = "Error"
        self.print_msg(class_name, func_name, level, msg)

    def print_msg(self, class_name, func_name, level, msg):
        now = datetime.now()
        msg_time = now.strftime(self.time_format)
        # 这里的日志归档可以再优化一下，现在不做，因为用不到这么细
        today = now.strftime("%Y-%m-%d")

        if not self.SLIENT:
            res_to_console = f"{self.TIME_COLOR + msg_time} {(self.LEVEL_COLOR[level] + level):10s} {self.NORMAL_COLOR}--- {f'class={class_name}, ' if class_name else ''}{f'func={func_name}: ' if func_name else ''}{self.LEVEL_COLOR[level] + msg}"
        else:
            res_to_console = f"{self.TIME_COLOR + msg_time} {(self.LEVEL_COLOR[level] + level):10s} {self.NORMAL_COLOR}--- {self.LEVEL_COLOR[level] + msg}"

        print(res_to_console)

        if self.output_to_file:
            # 没有开启静默模式
            if not self.SLIENT:
                res_to_file = f"{msg_time} {level:5s} --- {f'class={class_name}, ' if class_name else ''}func={func_name}: {msg}\n"
            else:
                # 开启静默模式
                res_to_file = f"{msg_time} {level:5s} --- {msg}\n"
            if not self.file_archive:
                with open(f"{self.dir_path}/log.txt", "a", encoding="utf-8") as f:
                    f.write(res_to_file)
            elif self.file_archive and not self.rolling_cutting:
                with open(f"{self.dir_path}/{today}.txt", "a", encoding="utf-8") as f:
                    f.write(res_to_file)
            elif self.file_archive and self.rolling_cutting:
                with open(f"{self.dir_path}/{today}_{self.rolling_cutting_index}.txt", "a", encoding="utf-8") as f:
                    f.write(res_to_file)
                size = os.path.getsize(f"{self.dir_path}/{today}_{self.rolling_cutting_index}.txt")
                if size >= self.file_max_size * 1024:
                    self.rolling_cutting_index += 1

    def get_calling_func_name(self):
        try:
            func_name = inspect.getframeinfo(inspect.currentframe().f_back.f_back)[2]
            return func_name if func_name != "<module>" else None
        except:
            return None

    def get_calling_class_name(self):
        try:
            return type(inspect.currentframe().f_back.f_back.f_locals["self"]).__name__
        except:
            return None

    def init_settings(self):
        if not os.path.exists("./log.properties"):
            return
        with open("./log.properties", "r", encoding="utf-8") as f:
            text = f.read()
            lines = text.split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith("#"):
                    continue
                if line.startswith("mode"):
                    self.mode = line.split("=")[1].strip()
                elif line.startswith("format_time"):
                    time_format = line.split("=")[1].strip()
                    self.time_format = time_format
                elif line.startswith("output_to_file"):
                    flag = line.split("=")[1].strip()
                    self.output_to_file = True if flag == "true" else False
                elif line.startswith("dir_path"):
                    self.dir_path = line.split("=")[1].strip()
                elif line.startswith("file_max_size"):
                    size = line.split("=")[1].strip()
                    if size.endswith("KB"):
                        self.file_max_size = int(size[:-2])
                    elif size.endswith("MB"):
                        self.file_max_size = int(size[:-2]) * 1024
                    elif size.endswith("GB"):
                        self.file_max_size = int(size[:-2]) * 1024 * 1024
                elif line.startswith("file_archive"):
                    flag = line.split("=")[1].strip()
                    self.file_archive = True if flag == "true" else False
                elif line.startswith("rolling_cutting"):
                    flag = line.split("=")[1].strip()
                    self.rolling_cutting = True if flag == "true" else False
                elif line.startswith("clear_pre_output"):
                    flag = line.split("=")[1].strip()
                    self.clear_pre_output = True if flag == "true" else False
        if self.output_to_file and self.clear_pre_output and os.path.exists(self.dir_path):
            # 清除之前的日志内容
            for file_name in os.listdir(self.dir_path):
                print(file_name)
                os.remove(f"{self.dir_path}/{file_name}")
        if self.output_to_file and not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)
        if self.output_to_file and self.file_archive and self.rolling_cutting:
            self.rolling_cutting_index = self.get_rolling_cutting_index()

    def get_rolling_cutting_index(self):
        today = datetime.now().strftime("%Y-%m-%d")
        index = 1
        if os.listdir(self.dir_path):
            for file_name in os.listdir(self.dir_path):
                if file_name.find(today) != -1 and file_name.find("_") != -1:
                    i = int(file_name[11:].split(".")[0])
                    index = max(index, i)
            # os.path.getsize 单位为B
            if os.path.exists(f"{self.dir_path}/{today}_{index}.txt"):
                size = os.path.getsize(f"{self.dir_path}/{today}_{index}.txt")
                if size >= self.file_max_size * 1024:
                    index += 1
        return index


log = MyLog()
