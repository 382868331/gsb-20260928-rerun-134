# 测试运行调度平台

这是一个 Go 实现的测试发现与执行调度核心：

- adapter.Registry 注册本地或后续接入的测试适配器。
- adapter.Case.Identity 是稳定用例身份；新适配器可绑定到同一身份，不改变历史归属。
- scheduler.Scheduler 将批次中的用例分配给多个工作者。
- 用例声明的资源在执行期间互斥；共享资源的用例串行执行。
- 工作者失联时用例记录为 uncertain，并保留其资源锁，直到显式恢复后释放。
- 取消 Context 只影响当前批次：未开始用例记录为 not_executed，之后的新批次仍可运行。
- History 返回每个用例的完整尝试记录；重试产生的新结果不会覆盖第一次失败。
- 报告分别统计 passed、failed、skipped、not_executed、uncertain。

本地内存适配器可用于组合可控的快慢测试。运行验证：

    go test -race ./...
    go vet ./...
