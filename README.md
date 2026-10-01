# testsched

`testsched` 是一个 Go 测试运行调度平台，负责测试发现、批次执行和结构化结果汇总。

## 能力

- 先注册内置 `local` 适配器；后续适配器通过相同 `Case.ID` 合并，始终保持同一用例身份。
- 一个批次可把用例分配给多个工作者并发运行；批次之间互不共享取消状态。
- `Case.Resources` 声明跨批次共享的稀缺设备；同一 `DevicePool` 内所有批次按 FIFO 公平排队，设备绝不会被两个执行同时占用。
- 用例可调用 `Execution.LoseWorker()` 模拟工作者失联；该次尝试记录为 `uncertain`，调度器用新工作者编号补满容量。失联时设备保持滞留（detained）：旧执行可能仍在运行，新批次和重试都不能立即取得设备。
- 运维人员确认旧执行确已结束后调用 `Scheduler.ConfirmResourceEnded(id, reason)` 解除占用；预约记录（`Reservation`）、原不确定尝试与人工理由永久保留在账目中。
- `Scheduler.ResourceStats()`、`DetainedReservations()`、`Reservations()` 提供设备占用/等待账目。
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
