from distutils.core import setup
from setuptools import find_packages

with open("readme.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="WkMysql",  # 包名
    version="1.0.0",  # 版本号
    description="Secondary encapsulation of pymysql and provision of thread pool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="WANGKANG",
    author_email="1686617586@qq.com",
    url="https://github.com/WANGKANG1717/WkLog",
    install_requires=["pymysql", "WkLog"],
    license="GPL-2.0",
    packages=find_packages(),
    platforms=["all"],
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
    require_python=">=3.10",
)
