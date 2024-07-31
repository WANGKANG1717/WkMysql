# -*- coding: utf-8 -*-
# @Date     : 2024-07-31 10:45:04
# @Author   : WANGKANG
# @Blog     : https://wangkang1717.github.io
# @Email    : 1686617586@qq.com
# @Filepath : setup.py
# @Brief    : 打包配置文件
# Copyright 2024 WANGKANG, All Rights Reserved.

# 更新软件包：pip install --upgrade setuptools wheel
# 安装twine: pipenv install twine --dev
# 运行命令: python setup.py upload

import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package meta-data.
NAME = "WkMysql"
DESCRIPTION = "The secondary encapsulation of pymysql provides a more user-friendly interface and thread pool, and will support SQLite in the future."
URL = "https://github.com/WANGKANG1717/WkDB"  # Github
# URL = "https://gitee.com/purify_wang/git_name" # Gitee
AUTHOR = "WANGKANG"
EMAIL = "1686617586@qq.com"
REQUIRES_PYTHON = ">=3.10.0"
LICENSE = "GPL-2.0"
PLATFORMS = ["all"]
INSTALL_REQUIRES = [
    # 'requests', 'maya', 'records',
]
EXTRAS_REQUIRE = {
    # 'fancy feature': ['django'],
}

VERSION = ""  # 为空自动加载包内__init__.py文件里的__version__变量
LONG_DESCRIPTION = ""  # 为空自动加载readme.md文件

here = os.path.abspath(os.path.dirname(__file__))


def load_version():
    """加载版本号"""
    global VERSION
    if not VERSION:
        project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
        with open(os.path.join(here, project_slug, "__init__.py")) as f:
            for line in f.readlines():
                if line.strip().startswith("__version__"):
                    VERSION = line.split("=")[1].strip().strip("'").strip('"')
                    break

    if not VERSION:
        raise RuntimeError("Cannot find version information")


def load_readme():
    """自动加载readme.md文件，如果没有则使用描述信息"""
    global LONG_DESCRIPTION
    try:
        with open(os.path.join(here, "readme.md"), encoding="utf-8") as f:
            LONG_DESCRIPTION = f.read()
    except FileNotFoundError:
        LONG_DESCRIPTION = DESCRIPTION


def print_info():
    print("=======================================================")
    print("Current Python Platform: ", sys.platform)
    print("Current Python Prefix: ", sys.prefix)
    print("Current Python Version: ", sys.version_info)
    print("Current Python Path: ", sys.executable)
    print("=======================================================")
    print("NAME: ", NAME)
    print("DESCRIPTION: ", DESCRIPTION)
    print("URL: ", URL)
    print("AUTHOR: ", AUTHOR)
    print("EMAIL: ", EMAIL)
    print("REQUIRES_PYTHON: ", REQUIRES_PYTHON)
    print("VERSION: ", VERSION)
    print("LICENSE: ", LICENSE)
    print("PLATFORMS: ", PLATFORMS)
    print("INSTALL_REQUIRES: ", INSTALL_REQUIRES)
    print("EXTRAS_REQUIRE: ", EXTRAS_REQUIRE)
    print("LONG_DESCRIPTION: ", LONG_DESCRIPTION)
    print("=======================================================")


load_version()
load_readme()


class UploadCommand(Command):
    """构建、并上传发布命令、推动标签到远程仓库"""

    user_options = []

    @staticmethod
    def status(s):
        """加粗输出文字"""
        print(f"\033[1m{s}\033[0m")

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        print_info()
        confirm = input("是否继续? yes/no:   ")
        if confirm.lower() != "yes":
            print("取消安装!")
            sys.exit(0)

        try:
            self.status("1. 移除之前的构建文件…")
            rmtree(os.path.join(here, "dist"))
            rmtree(os.path.join(here, "build"))
            rmtree(os.path.join(here, "*.egg-info"))
        except OSError:
            pass

        self.status("2. 构建Source and Wheel (universal)文件…")
        flag = os.system(f"{sys.executable} setup.py sdist bdist_wheel --universal")
        if flag != 0:
            raise RuntimeError("构建失败!")

        self.status("3. 通过Twine上传到PyPI…")
        flag = os.system("twine upload dist/*")
        if flag != 0:
            raise RuntimeError("上传失败!")

        self.status("4. 推送标签到远程仓库…")
        flag = os.system(f"git tag v{VERSION}")
        if flag != 0:
            raise RuntimeError("创建标签失败!")
        flag = os.system("git push --tags")
        if flag != 0:
            raise RuntimeError("推送标签失败!")

        sys.exit()


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=EMAIL,
    url=URL,
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],
    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    include_package_data=True,
    license=LICENSE,
    platforms=PLATFORMS,
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Natural Language :: Chinese (Simplified)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries",
    ],
    cmdclass={
        "upload": UploadCommand,
    },
)
