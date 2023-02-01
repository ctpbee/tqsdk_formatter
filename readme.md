### tqsdk formatter

快速简单易用的天勤数据格式转换器, 可为`ctpbee`研究所需要的`tick`格式

#### 快速开始

```bash
cargo run --release  -- -f Z:\delete -w csv -t ./tick -c true -n 10 
```

#### 参数解析:

- `-n`: 使用多少个线程并发处理,默认为5
- `-t`: 输出目标路径的,默认参数为`./dir`, 可以用使用绝对路径或者相对路径
- `-f`: `tqsdk`下载数据的`csv`路径, 默认在当前路径下面
- `-w`: 文件输出格式,暂时只支持`["csv","parquet"]`
- `-c`: 是否自动创建输出路径, 支持`false`和`true`

> PS:因为是一次性脚本,对很多变量使用`clone`, 不喜勿喷.