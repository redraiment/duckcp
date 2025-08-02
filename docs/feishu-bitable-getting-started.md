飞书多维表格配置
====

本文档介绍如何在飞书开放平台上创建应用，以及关联授权该应用通过接口编辑多维表格。

## 前置准备

1. 首先进入[飞书主页](https://www.feishu.cn)注册一个飞书账号。
2. 然后创建一个新的企业（不需要认证）；或者加入已注册的企业，并获得开发者权限。
3. 接着可以按照下文的步骤创建企业应用并关联多维表格。

## 一、创建企业应用

1. 进入[飞书开发者后台](https://open.feishu.cn/app)页面，如下图所示。
2. 点击『创建企业自建应用』按钮，填写信息并点击『创建』。

![创建企业自建应用](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-create.png)

## 二、添加应用权限

新创建的应用没有任何权限，duckcp同步数据需要调用以下三个接口：

1. [查询记录接口](https://open.feishu.cn/document/docs/bitable-v1/app-table-record/search)
2. [新增多条记录接口](https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_create)
3. [删除多条记录接口](https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_delete)

若当前应用尚未获得相关的权限，右上角『权限配置』选项卡就会出现红色感叹号，如下图所示：

![接口列表](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-apis.png)

切换至『权限配置』选项卡，选中状态是『未开通』的选项，点击右上角的『批量开通』——如下图所示——并在弹出窗口中点击『开通』

![批量开通权限](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-grant-authorities.png)

**特别注意**：开通权限前先确定当前使用的Access Token是否为『Tenant Access Token』；若不是，点击『切换为tenant_access_token』按钮。如下图所示：

![切换访问凭证类型](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-tenant_access-token.png)

在开放平台后台的应用详情页面中，切换到『开发配置』-『权限管理』，能看到当前应用申请的所有权限。如下图所示：

![权限管理](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-permissions.png)

## 三、创建应用版本

应用需要创新新版本并发布，申请的权限才能生效。

在开放平台后台的应用详情页面中，切换到『应用发布』-『版本管理与发布』，再点击右上角的『创建版本』按钮，就能新建版本。如下图所示：

![创建应用版本](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/app-version.png)

## 四、创建多维表格

1. 进入[飞书企业云盘](https://www.feishu.cn/product/drive)产品页面。
2. 点击『新建』-『多维表格』按钮。
3. 在弹出的窗口里点击『新建空白表格』按钮。

## 五、添加文档应用

1. 点击右上角『...』按钮。
2. 进入『... 更多』菜单。
3. 点击『添加文档应用』。

![添加文档应用](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/bitable-add-app.png)

在弹出的『文档应用』窗口中搜索上面创建的企业应用名称，并为其添加『可编辑』权限。如下图所示：

![搜索应用](https://raw.githubusercontent.com/redraiment/duckcp/master/docs/bitable-search-app.png)
