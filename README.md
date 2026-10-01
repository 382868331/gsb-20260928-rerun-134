# testsched

`testsched` 是一个 Go 测试运行调度平台，负责测试发现、批次执行和结构化结果汇总。

## 能力

- 先注册内置 `local` 适配器；后续适配器通过相同 `Case.ID` 合并，始终保持同一用例身份。
- 一个批次可把用例分配给多个工作者并发运行；批次之间互不共享取消状态。
- `Case.Resources` 声明共享稀缺设备，预约由调度器统一记账：跨批次竞争同一资源时保持互斥，同一用例的多资源一次性原子获取，先到的竞争者优先（公平）。
- 用例可调用 `Execution.LoseWorker()` 模拟工作者失联；该次尝试记录为 `uncertain`，调度器用新工作者编号补满容量。失联时用例可能仍在执行，其资源占用保留并标记为不确定，不会立即交给下一批；人工确认旧执行已结束后调用 `Scheduler.ConfirmFinished(batchID, caseID)` 解除占用，返回被释放的预约作为依据，原不确定尝试保留不改。
- `Scheduler.Reservations()` 返回当前所有批次的资源占用快照，用于核对资源账目。
- 失败和失联结果可配置重试；`Record.Attempts` 只追加，重试成功也保留首次失败或失联记录。
- 取消批次时，运行中与未派发的用例都记录为 `not_run`，不会误记为失败或跳过。
- 程序应断言 `Result`、`Record` 和 `Counts`；`Result.Report()` 仅提供给人阅读的摘要。

```go
cases := []testsched.Case{
    {ID: "math/add", Run: func(context.Context, *testsched.Execution) testsched.Status {
        return testsched.StatusPass
    }},
}
registry := testsched.NewRegistry(testsched.LocalAdapter(cases))
scheduler := testsched.New(registry, testsched.DefaultConfig())
result, discoveryErrors := scheduler.Batch(context.Background())
counts := result.Counts()
```
