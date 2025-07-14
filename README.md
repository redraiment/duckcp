DuckCP
=====

同步数据的小工具，支持以下类型的数据源之间同步数据：

- PostgreSQL数据库：以及兼容的数据库。例如Hologres。
- MaxCompute数据库：即ODPS。
- SQLite数据库：文件型OLTP数据库。
- DuckDB数据库：文件型OLAP数据库。
- 本地文件：基于DuckDB实现，支持CSV、Parquet、JSON三种格式的读写。
- 飞书多维表格：基于DuckDB实现。多维表格被映射成DuckDB的只读View，可执行SQL查询。

## 一、功能简介

DuckCP按照以下步骤同步数据：

1. 连接数据源，并执行SQL查询数据。
2. **清空**目标存储单元里的存量数据。
3. 将查询结果批量保存到目标存储单元内。

## 二、安装与使用

* 安装方法：`pip install duckcp==0.1.0`
* 使用方法：`duckcp -h`

## 三、案例演示

假设在`data`目录下有一个`programmers.csv`文件，保存了每位程序员使用的编程语言。内容如下：

| id | name    | language   |
|----|---------|------------|
| 1  | Joe     | Java       |
| 2  | Alice   | JavaScript |
| 3  | Leon    | C/C++      |
| 4  | William | Java       |
| 5  | James   | C/C++      |
| 6  | Enson   | C/C++      |

本案例将演示从上述CSV文件中读取数据，统计使用各种编程语言的程序员人数，并将统计结果保存到飞书多维表格中，表格包含以下两个字段：

1. 编程语言：文本类型。
2. 程序员人数：整数类型。

如下图所示：

![多维表格字段](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/bitable-fields.png)

多维表格将自动基于统计结果绘制图表，如下图所示：

![多维表格图表](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/bitable-chart.png)

### 3.1 初始化

DuckCP在本地SQLite3数据库中管理以下元信息：

- 数据仓库（Repository）：定义各类数据库的连接信息。
- 存储单元（Storage）：定义数据仓库内的存储单元。例如数据库的表、目录下的文件等。
- 迁移任务（Transformer）：定义来源仓库、目标仓库、迁移脚本（SQL）等迁移信息。
- 迁移作业（Task）：定义可同时执行的迁移任务，及其执行顺序。

使用以下命令创建元信息数据库：

```shell
duckcp meta create
```

元信息数据库默认保存在以下路径：

* Linux系统：`$HOME/.config/com.yinfn.duckcp/configuration.db`
* macOS系统：`$HOME/Library/Application\ Support/com.yinfn.duckcp/configuration.db`
* Windows系统：`%LOCALAPPDATA%\com.yinfn.duckcp\configuration.db`

可以通过全局选项`-c/--config-file`指定数据库文件路径：

```shell
duckcp -c <PATH> meta create
```

### 3.2 创建数据仓库

本案例中需要创建两个数据仓库：

1. 文件类型（kind=`file`）数据仓库：即前文中数据文件所在的目录`data`。取名为『文件仓库』。
2. 多维表格（kind=`bitable`）数据仓库：可管理多维表格的飞书开放平台应用。取名为『多维表格』。

命令如下：

```shell
duckcp repository create 文件仓库 -k file --folder data
duckcp repository create 多维表格 -k bitable --access-key <APP-ID> --access-secret <APP-SECRET>
```

其中文件类型（`-k file`）仓库的连接选项：

- `--folder <FOLDER>`：CSV等数据文件所在的目录。本例中『FOLDER』为『data』。

多维表格类型（`-k bitable`）仓库的连接选项：

- `--access-key <APP-ID>`：飞书开放平台中应用凭证的『App ID』。
- `--access-secret <APP-SECRET>`：飞书开放平台中应用凭证的『App Secret』。

飞书开放平台上应用凭证的获取方式如下图所示：

![多维表格图表](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/feishu-open-platform-credentials.png)

不同类型的仓库连接选项不一样，细节请参见`duckcp repository create -h`。

### 3.3 创建存储单元

作为数据来源的『文件仓库』不需要创建存储单元；作为存储目标，数据仓库『多维表格』必须创建具体的『存储单元』，即目标最终保存在仓库的那张表或哪个文件内。

本例中，数据最终保存到多维表格『程序员分布表』数据表中。创建存储单元的方法如下：

```shell
duckcp storage create 程序员分布表 -r 多维表格 --document ZLtubKG26aGqnmsqq5Cc8YCBnmo --table tblFkmAFDURXSSop
```

存储介质选项包括：

- `-r/--repository <REPOSITORY>`：指定所属的数据仓库。本例中『REPOSITORY』为『多维表格』。
- `--document <DOCUMENT>`：飞书多维表格文档的编码。获取方式参考下方，本例中『DOCUMENT』为『ZLtubKG26aGqnmsqq5Cc8YCBnmo』。
- `--table <TABLE>`：飞书多维表格数据表的编码。获取方式参考下方，本例中『TABLE』为『tblFkmAFDURXSSop』。

本例中，多维表格数据表网站的URL为：

```
https://yinfn-tech.feishu.cn/base/ZLtubKG26aGqnmsqq5Cc8YCBnmo?table=tblFkmAFDURXSSop&view=vewRcgeNX6
```

- `/base/`之后的路径参数就是文档编码。
- 查询参数`table`的值就是数据表编码。

### 3.4 创建迁移

创建迁移之前，首先需要创建一个SQL迁移脚本。在本例中我在`data`目录下创建了一个`迁移脚本.sql`文件，内容如下：

```sql
select
  "language" as "编程语言",
  count(*) as "程序员人数"
from
  read_csv('programmers.csv')
group by
  "language"
order by
  "程序员人数" desc
```

DuckCP的文件类型仓库本质上是一个临时的DuckDB数据库，因此读取CSV使用DuckDB内置的`read_csv`函数。

接着可以创建迁移，指定从文件数据源中用迁移脚本读取数据，并保存至多维表格的数据表中。方法如下：

```shell
duckcp transformer create 数据统计 -s 文件仓库 -t 多维表格 -o 程序员分布表 -f data/迁移脚本.sql
```

其中选项包括：

- `-s/--source REPOSITORY`：指定来源数据仓库。本例中『REPOSITORY』为『文件仓库』。
- `-t/--target REPOSITORY`：指定目标数据仓库。本例中『REPOSITORY』为『多维表格』。
- `-o/--storage STORAGE`：指定目标存储单元。本例中『STORAGE』为『多维表格』的『程序员分布表』。
- `-f/--script FILE`：指定迁移脚本，用于从来源数据仓库内读取数据和加工数据。本例中『FILE』为『data/迁移脚本.sql』。

### 3.5 执行迁移

最后，可以执行前文创建的迁移。方法如下：

```shell
duckcp transformer execute 数据统计
```

## 问题反馈

DuckCP在2023年9月开始在公司内部使用，前后重写超过6次，近期（2025年6月）才开始筹备开源。
代码难免有错误和不足，欢迎在Github上提交问题，或发邮件至 redraiment@gmail.com 交流。

感谢！
