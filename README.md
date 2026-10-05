# testsched

`testsched` 是一个 Go 测试运行调度平台，负责测试发现、批次执行和结构化结果汇总。

## 能力

- 先注册内置 `local` 适配器；后续适配器通过相同 `Case.ID` 合并，始终保持同一用例身份。
- 一个批次可把用例分配给多个工作者并发运行；批次之间互不共享取消状态。
- `Case.Resources` 声明共享稀缺设备，预约由调度器统一记账：跨批次竞争同一资源时保持互斥，同一用例的多资源一次性原子获取，先到的竞争者优先（公平）。
- 设备组合只有完整可用才开始执行，等待中的组合不会预占部分设备。`Scheduler.Waiting()` 解释每个等待者：还缺哪些设备、被哪个批次/用例占用、占用是否处于失联不确定状态。相互交叉的组合按到达顺序公平推进，互不相交的请求互不阻塞。
- 等待期间可用 `Scheduler.ShrinkWait(batchID, caseID, keep)` 把需求缩减为原组合的子集，或用 `Scheduler.CancelWait(batchID, caseID)` 取消等待（该用例记为 `not_run`）；两者只影响仍在等待的请求，已交给执行者的设备仍按真实状态记账。
- 用例可调用 `Execution.LoseWorker()` 模拟工作者失联；该次尝试记录为 `uncertain`，调度器用新工作者编号补满容量。失联时用例可能仍在执行，其资源占用保留并标记为不确定，不会立即交给下一批；人工确认旧执行已结束后调用 `Scheduler.ConfirmFinished(batchID, caseID)` 解除占用，返回被释放的预约作为依据，原不确定尝试保留不改。
- `Scheduler.Reservations()` 返回当前所有批次的资源占用快照，用于核对资源账目。
- `Scheduler.Simulate(req)` 提供等待队列的只读推演：假设指定占用已结束（`ReleaseOwners`）或指定等待者已缩减（`Shrinks`），返回哪些完整组合可以继续、哪些仍被剩余占用或先到的交叉组合阻挡。推演在账目副本上进行，不实际占用、释放或缩减任何资源，也不会推进账目版本。`Scheduler.LedgerVersion()` 暴露当前账目版本；`Scheduler.SaveSimulation`/`LoadSimulation` 保存的推演自带推演时的账目版本，账目变化后调用 `Scheduler.SimulationStatus(sim)` 可得知哪些结论（哪个等待者的继续/阻挡判定）因占用或等待者变化而失效、需要重新推演。
- `Scheduler.SaveReservations(w)` / `Scheduler.LoadReservations(r)` 以 JSON 保存并恢复资源账目；重开后归属与失联标记保持一致，`ConfirmFinished` 仍释放同一份占用。恢复时设备已被他人占用则拒绝覆盖并返回冲突清单。重复确认、缩减与取消均为幂等，不会让一台设备同时属于两个执行，也不会抹去失联事实。
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
